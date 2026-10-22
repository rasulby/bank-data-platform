import os
import random
from datetime import timedelta

from faker import Faker
from psycopg2.extras import execute_values
from db import get_conn

N_CUSTOMERS = int(os.getenv("SEED_CUSTOMERS", "1000"))
N_MERCHANTS = 200

random.seed(42)          # Use a fixed seed for repeatable sample data
Faker.seed(42)
fake = Faker("az_AZ")

CITIES = ["Baku", "Sumgait", "Ganja", "Mingachevir", "Lankaran", "Shaki", "Nakhchivan", "Quba"]
CITY_WEIGHTS = [60, 12, 8, 4, 4, 4, 4, 4]
SEGMENTS = ["MASS", "AFFLUENT", "PREMIUM"]
SEGMENT_WEIGHTS = [80, 15, 5]
CURRENCIES = ["AZN", "USD", "EUR"]
CURRENCY_WEIGHTS = [85, 10, 5]
MERCHANT_TYPES = [
    ("Supermarket", "5411"), ("Restaurant", "5812"), ("Fuel station", "5541"),
    ("Pharmacy", "5912"), ("Electronics", "5732"), ("Shop", "5311"),
    ("Taxi", "4121"), ("Hotel", "7011"),
]
PHONE_PREFIXES = ["50", "51", "55", "70", "77", "99", "10"]




def make_phone():
    return f"+994{random.choice(PHONE_PREFIXES)}{random.randint(1000000, 9999999)}"


def make_iban():
    # AZ + 2 digits + SYNT + 20 digits = 28 characters
    digits = "".join(random.choices("0123456789", k=20))
    return f"AZ{random.randint(10, 99)}SYNT{digits}"


def make_card_number():
    # Start with 9 to mark synthetic card numbers
    return "9" + "".join(random.choices("0123456789", k=15))


def main():
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM customers")
            if cur.fetchone()[0] > 0:
                print("Data already exists; skipping seed.")
                return

            # 1) Customers
            rows = []
            for _ in range(N_CUSTOMERS):
                created = fake.date_time_between(start_date="-5y", end_date="-30d")
                rows.append((
                    fake.name(),
                    make_phone(),
                    random.choices(CITIES, CITY_WEIGHTS)[0],
                    random.choices(SEGMENTS, SEGMENT_WEIGHTS)[0],
                    created,
                    created,
                ))
            customers = execute_values(
                cur,
                """INSERT INTO customers
                   (full_name, phone, city, segment, created_at, updated_at)
                   VALUES %s RETURNING customer_id, created_at""",
                rows,
                fetch=True,
            )

            # 2) Accounts (1-2 per customer)
            rows = []
            for customer_id, created_at in customers:
                for _ in range(random.choices([1, 2], [75, 25])[0]):
                    opened = created_at + timedelta(days=random.randint(0, 7))
                    rows.append((
                        customer_id,
                        make_iban(),
                        random.choices(CURRENCIES, CURRENCY_WEIGHTS)[0],
                        round(random.lognormvariate(6, 1.2), 2),
                        opened,
                        opened,
                    ))
            accounts = execute_values(
                cur,
                """INSERT INTO accounts
                   (customer_id, iban, currency, balance, opened_at, updated_at)
                   VALUES %s RETURNING account_id, opened_at""",
                rows,
                fetch=True,
            )

            # 3) Cards (one per account)
            rows = []
            for account_id, opened_at in accounts:
                rows.append((
                    account_id,
                    make_card_number(),
                    random.choices(["ACTIVE", "BLOCKED"], [95, 5])[0],
                    (opened_at + timedelta(days=365 * 4)).date(),
                    opened_at,
                    opened_at,
                ))
            execute_values(
                cur,
                """INSERT INTO cards
                   (account_id, card_number, status, expires_at, created_at, updated_at)
                   VALUES %s""",
                rows,
            )

            # 4) Merchants
            rows = []
            for i in range(1, N_MERCHANTS + 1):
                label, mcc = random.choice(MERCHANT_TYPES)
                city = random.choices(CITIES, CITY_WEIGHTS)[0]
                rows.append((f"{label} {city} #{i:03d}", mcc, city))
            execute_values(
                cur,
                "INSERT INTO merchants (name, mcc, city) VALUES %s",
                rows,
            )

            print(f"OK: {len(customers)} customers, {len(accounts)} accounts, "
                  f"{len(accounts)} cards, {N_MERCHANTS} merchant")
    finally:
        conn.close()


if __name__ == "__main__":
    main()