CREATE TABLE customers (
    customer_id  BIGSERIAL PRIMARY KEY,
    full_name    TEXT        NOT NULL,
    phone        TEXT        NOT NULL,
    city         TEXT        NOT NULL,
    segment      TEXT        NOT NULL CHECK (segment IN ('MASS','AFFLUENT','PREMIUM')),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    account_id   BIGSERIAL PRIMARY KEY,
    customer_id  BIGINT        NOT NULL REFERENCES customers(customer_id),
    iban         TEXT          NOT NULL UNIQUE,
    currency     TEXT          NOT NULL CHECK (currency IN ('AZN','USD','EUR')),
    balance      NUMERIC(18,2) NOT NULL DEFAULT 0,
    status       TEXT          NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE','FROZEN','CLOSED')),
    opened_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE cards (
    card_id      BIGSERIAL PRIMARY KEY,
    account_id   BIGINT      NOT NULL REFERENCES accounts(account_id),
    card_number  TEXT        NOT NULL UNIQUE,
    status       TEXT        NOT NULL DEFAULT 'ACTIVE'
                 CHECK (status IN ('ACTIVE','BLOCKED','EXPIRED')),
    expires_at   DATE        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    merchant_id  BIGSERIAL PRIMARY KEY,
    name         TEXT    NOT NULL,
    mcc          CHAR(4) NOT NULL,
    city         TEXT    NOT NULL
);

CREATE TABLE transactions (
    txn_id        BIGSERIAL PRIMARY KEY,
    card_id       BIGINT        NOT NULL REFERENCES cards(card_id),
    merchant_id   BIGINT        REFERENCES merchants(merchant_id),
    txn_type      TEXT          NOT NULL
                  CHECK (txn_type IN ('PURCHASE','ATM_WITHDRAWAL','TRANSFER')),
    amount        NUMERIC(18,2) NOT NULL CHECK (amount > 0),
    currency      TEXT          NOT NULL,
    status        TEXT          NOT NULL CHECK (status IN ('SUCCESS','DECLINED')),
    country_code  CHAR(2)       NOT NULL,
    created_at    TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_txn_card_created ON transactions (card_id, created_at);
CREATE INDEX idx_txn_created      ON transactions (created_at);