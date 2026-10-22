import argparse
import random
import time
from datetime import datetime, timezone

from db import get_conn

COUNTRIES_ABROAD = ["TR", "GE", "RU", "AE", "DE"]
FRAUD_COUNTRIES = ["TR", "AE", "DE", "GE"]
SEGMENTS = ["MASS", "AFFLUENT", "PREMIUM"]
ATM_AMOUNTS = [20, 50, 100, 200, 500]


def now_utc():
    return datetime.now(timezone.utc)


def load_state(cur):
    cur.execute("""
        SELECT c.card_id, c.account_id, a.currency
        FROM cards c JOIN accounts a USING (account_id)
        WHERE c.status = 'ACTIVE' AND a.status = 'ACTIVE'
    """)
    cards = cur.fetchall()
    cur.execute("SELECT merchant_id FROM merchants")
    merchants = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT customer_id FROM customers")
    customers = [r[0] for r in cur.fetchall()]
    return cards, merchants, customers


def insert_txn(cur, card, merchant_id, txn_type, amount, country):
    card_id, account_id, currency = card
    # Subtract the amount only when the balance is sufficient
    cur.execute(
        """UPDATE accounts
           SET balance = balance - %s, updated_at = %s
           WHERE account_id = %s AND balance >= %s
           RETURNING account_id""",
        (amount, now_utc(), account_id, amount),
    )
    status = "SUCCESS" if cur.fetchone() else "DECLINED"
    cur.execute(
        """INSERT INTO transactions
           (card_id, merchant_id, txn_type, amount, currency,
            status, country_code, created_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (card_id, merchant_id, txn_type, amount, currency,
         status, country, now_utc()),
    )
    return status


def normal_txn(cur, cards, merchants):
    card = random.choice(cards)
    kind = random.choices(
        ["PURCHASE", "ATM_WITHDRAWAL", "TRANSFER"], [80, 12, 8]
    )[0]
    if kind == "PURCHASE":
        amount = max(1.0, round(random.lognormvariate(3, 1), 2))
        merchant = random.choice(merchants)
    elif kind == "ATM_WITHDRAWAL":
        amount = random.choice(ATM_AMOUNTS)
        merchant = None
    else:
        amount = max(1.0, round(random.lognormvariate(4.5, 1.2), 2))
        merchant = None
    country = "AZ" if random.random() < 0.97 else random.choice(COUNTRIES_ABROAD)
    insert_txn(cur, card, merchant, kind, amount, country)


def fraud_velocity(cur, cards, merchants):
    """Generate transactions in three countries for the same card."""
    card = random.choice(cards)
    for country in random.sample(FRAUD_COUNTRIES, 3):
        amount = round(random.uniform(300, 900), 2)
        insert_txn(cur, card, random.choice(merchants), "PURCHASE", amount, country)


def fraud_large_amount(cur, cards, merchants):
    """Generate an unusually large transaction."""
    card = random.choice(cards)
    amount = round(random.uniform(2000, 6000), 2)
    insert_txn(cur, card, random.choice(merchants), "PURCHASE", amount, "AZ")


def maintenance(cur, cards, customers):
    """Update bank records to produce CDC update events."""
    r = random.random()
    if r < 0.05:  # Change the customer segment
        cur.execute(
            "UPDATE customers SET segment = %s, updated_at = %s WHERE customer_id = %s",
            (random.choice(SEGMENTS), now_utc(), random.choice(customers)),
        )
    elif r < 0.06 and len(cards) > 10:  # Block a card
        card = cards.pop(random.randrange(len(cards)))
        cur.execute(
            "UPDATE cards SET status = 'BLOCKED', updated_at = %s WHERE card_id = %s",
            (now_utc(), card[0]),
        )
    elif r < 0.26:  # Deposit into an account
        card = random.choice(cards)
        cur.execute(
            "UPDATE accounts SET balance = balance + %s, updated_at = %s WHERE account_id = %s",
            (round(random.uniform(100, 2000), 2), now_utc(), card[1]),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=5, help="Transactions per second")
    parser.add_argument("--duration", type=int, default=0, help="Duration in seconds (0 = unlimited)")
    args = parser.parse_args()

    conn = get_conn()
    with conn.cursor() as cur:
        cards, merchants, customers = load_state(cur)
    conn.commit()
    print(f"Started: {len(cards)} active cards, rate={args.rate}/sec. Stop: Ctrl+C")

    started = time.time()
    total = 0
    try:
        while args.duration == 0 or time.time() - started < args.duration:
            tick_start = time.time()
            with conn.cursor() as cur:
                for _ in range(args.rate):
                    normal_txn(cur, cards, merchants)
                    total += 1
                if random.random() < 0.02:
                    fraud_velocity(cur, cards, merchants)
                    print("[fraud] velocity")
                if random.random() < 0.02:
                    fraud_large_amount(cur, cards, merchants)
                    print("[fraud] large amount")
                maintenance(cur, cards, customers)
            conn.commit()  # Commit each batch as one transaction
            time.sleep(max(0, 1 - (time.time() - tick_start)))
    except KeyboardInterrupt:
        print(f"\nStopped. Normal transactions: {total}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()