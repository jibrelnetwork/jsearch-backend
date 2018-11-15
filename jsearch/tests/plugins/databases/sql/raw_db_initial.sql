CREATE TABLE IF NOT EXISTS headers (
    block_number bigint UNIQUE,
    block_hash varchar(70),
    fields jsonb
);

CREATE TABLE IF NOT EXISTS  bodies (
    block_number bigint UNIQUE,
    block_hash varchar(70),
    fields jsonb
);

CREATE TABLE IF NOT EXISTS pending_transactions (
    tx_hash varchar(70) UNIQUE,
    status varchar,
    fields jsonb
);

CREATE TABLE IF NOT EXISTS receipts (
    block_number bigint UNIQUE,
    block_hash varchar(70),
    fields jsonb
);

CREATE TABLE IF NOT EXISTS accounts (
    block_number bigint,
    block_hash varchar(70),
    address varchar(45),
    fields jsonb,
    UNIQUE(block_number, address)
);

CREATE TABLE IF NOT EXISTS rewards (
    block_number bigint UNIQUE,
    block_hash varchar(70),
    address varchar(45),
    fields jsonb
);

CREATE TABLE IF NOT EXISTS internal_transactions (
    id bigserial primary key,
    block_number bigint,
    type varchar(20),
    timestamp bigint,
    fields jsonb
);

CREATE INDEX IF NOT EXISTS internal_transactions_block_number ON internal_transactions(block_number);

CREATE INDEX IF NOT EXISTS pending_transactions_status ON pending_transactions(status);

CREATE INDEX IF NOT EXISTS internal_transactions_type ON internal_transactions(type);

CREATE INDEX IF NOT EXISTS internal_transactions ON internal_transactions(block_number);
