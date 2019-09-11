CREATE TABLE IF NOT EXISTS headers (
    block_number bigint,
    block_hash varchar(70) UNIQUE,
    fields jsonb
);

CREATE TABLE IF NOT EXISTS bodies (
    block_number bigint,
    block_hash varchar(70) UNIQUE,
    fields jsonb
);

CREATE TABLE IF NOT EXISTS pending_transactions (
    tx_hash varchar(70) NOT NULL,
    status varchar,
    fields jsonb,
    id bigint NOT NULL,
    "timestamp" timestamp,
    removed boolean,
    node_id varchar(70)
);

CREATE TABLE IF NOT EXISTS receipts (
    block_number bigint,
    block_hash varchar(70) UNIQUE,
    fields jsonb
);

CREATE TABLE IF NOT EXISTS accounts (
    id bigserial primary key,
    block_number bigint,
    block_hash varchar(70),
    address varchar(45),
    fields jsonb,
    UNIQUE(block_hash, address)
);

CREATE TABLE IF NOT EXISTS rewards (
    id bigserial primary key,
    block_number bigint,
    block_hash varchar(70) UNIQUE,
    address varchar(45),
    fields jsonb
);

CREATE TABLE IF NOT EXISTS internal_transactions (
    id bigserial primary key,
    block_number bigint,
    block_hash varchar(70),
    parent_tx_hash varchar(70),
    index bigint,
    type varchar(20),
    timestamp bigint,
    fields jsonb
);

CREATE TABLE IF NOT EXISTS chain_splits (
    id bigserial primary key,
    common_block_number bigint,
    common_block_hash varchar(70),
    drop_length bigint,
    drop_block_hash varchar(70),
    add_length bigint,
    add_block_hash varchar(70),
    node_id varchar(70)
);

CREATE TABLE IF NOT EXISTS reorgs (
    id bigserial primary key,
    block_number bigint,
    block_hash varchar(70),
    header jsonb,
    reinserted boolean,
    split_id bigint,
    node_id varchar(70)
);

CREATE TABLE IF NOT EXISTS chain_events (
    id bigint primary key,
    block_number bigint,
    block_hash  varchar(70),
    parent_block_hash varchar(70),
    type varchar(20),
    common_block_number bigint,
    common_block_hash  varchar(70),
    drop_length bigint,
    drop_block_hash varchar(70),
    add_length bigint,
    add_block_hash varchar(70),
    node_id varchar(70),
    created_at timestamp
);

CREATE TABLE IF NOT EXISTS token_holders (
    id bigserial primary key,
    block_number bigint NOT NULL,
    block_hash varchar(70) NOT NULL,
    token_address varchar(45) NOT NULL,
    holder_address varchar(45) NOT NULL,
    balance numeric NOT NULL,
    UNIQUE(block_hash, token_address, holder_address)
);



CREATE INDEX IF NOT EXISTS ix_internal_transactions_block_number ON internal_transactions(block_number);

CREATE UNIQUE INDEX IF NOT EXISTS internal_transactions_uniq ON internal_transactions(block_hash, parent_tx_hash, index, type);

CREATE INDEX IF NOT EXISTS ix_pending_transactions_status ON pending_transactions(status);

CREATE INDEX IF NOT EXISTS ix_internal_transactions_type ON internal_transactions(type);

CREATE INDEX IF NOT EXISTS ix_internal_transactions_block_hash ON internal_transactions(block_hash);

CREATE INDEX IF NOT EXISTS ix_rewards_block_number ON rewards(block_number);

CREATE INDEX IF NOT EXISTS ix_rewards_block_hash ON rewards(block_hash);

CREATE INDEX IF NOT EXISTS ix_accounts_block_number ON accounts(block_number);

CREATE INDEX IF NOT EXISTS ix_accounts_block_hash ON accounts(block_hash);

CREATE INDEX IF NOT EXISTS ix_receipts_block_number ON receipts(block_number);

CREATE INDEX IF NOT EXISTS ix_receipts_block_hash ON receipts(block_hash);

CREATE INDEX IF NOT EXISTS ix_bodies_block_number ON bodies(block_number);

CREATE INDEX IF NOT EXISTS ix_bodies_block_hash ON bodies(block_hash);

CREATE INDEX IF NOT EXISTS ix_headers_block_number ON headers(block_number);

CREATE INDEX IF NOT EXISTS ix_headers_block_hash ON headers(block_hash);

CREATE INDEX IF NOT EXISTS ix_chain_splits_common_block_number ON chain_splits(common_block_number);

CREATE INDEX IF NOT EXISTS ix_reorgs_block_number ON reorgs(block_number);

CREATE INDEX IF NOT EXISTS ix_reorgs_block_hash ON reorgs(block_hash);

CREATE INDEX IF NOT EXISTS ix_reorgs_reinserted ON reorgs(reinserted);