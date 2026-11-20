CREATE TABLE accounts (
  account_id       INT PRIMARY KEY,
  opened_on        DATE,
  balance          NUMERIC(12,2),
  updated_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE transactions (
  transaction_id   BIGINT PRIMARY KEY,
  account_id       INT REFERENCES accounts(account_id),
  transaction_date DATE,
  amount           NUMERIC(12,2),
  operation        TEXT,
  balance_after    NUMERIC(12,2)
);

CREATE TABLE replay_state (last_replayed_day DATE);
INSERT INTO replay_state VALUES (NULL);

-- Include the old values in update events, not just the new ones
ALTER TABLE accounts REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;

-- A user just for Debezium: can read and replicate, nothing else
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium-local-only';
GRANT SELECT ON accounts, transactions TO debezium;

-- The list of tables Debezium is allowed to watch (replay_state is deliberately left out)
CREATE PUBLICATION dbz_bank FOR TABLE accounts, transactions;