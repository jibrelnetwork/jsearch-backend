"""start

Revision ID: 9e7a95e13e7d
Revises: 
Create Date: 2018-12-12 13:13:39.023861

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '9e7a95e13e7d'
down_revision = None
branch_labels = None
depends_on = None


UP_SQL = """
CREATE TABLE accounts_base (
    address character varying NOT NULL,
    code character varying,
    code_hash character varying,
    last_known_balance numeric(32,0),
    root character varying
);
CREATE INDEX ix_accounts_base_address ON accounts_base USING btree (address);


CREATE TABLE accounts_state (
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    address character varying NOT NULL,
    nonce integer,
    root character varying,
    balance numeric(32,0),
    is_forked boolean default false
);
ALTER TABLE ONLY accounts_state
    ADD CONSTRAINT accounts_state_pkey PRIMARY KEY (block_hash, address);
CREATE INDEX ix_accounts_state_block_number ON accounts_state USING btree (block_number);
CREATE INDEX ix_accounts_state_is_forked ON accounts_state USING btree (is_forked);
CREATE INDEX ix_accounts_state_address_block_number ON accounts_state USING btree (address, block_number);


CREATE TABLE blocks (
    number integer NOT NULL,
    hash character varying NOT NULL,
    parent_hash character varying,
    difficulty bigint,
    extra_data character varying,
    gas_limit bigint,
    gas_used bigint,
    logs_bloom character varying,
    miner character varying,
    mix_hash character varying,
    nonce character varying,
    receipts_root character varying,
    sha3_uncles character varying,
    size integer,
    state_root character varying,
    "timestamp" integer,
    total_difficulty bigint,
    transactions_root character varying,
    is_sequence_sync boolean default false,
    static_reward numeric(32,0),
    uncle_inclusion_reward numeric(32,0),
    tx_fees numeric(32,0),
    is_forked boolean default false
);
ALTER TABLE ONLY blocks
    ADD CONSTRAINT blocks_pkey PRIMARY KEY (hash);
CREATE INDEX ix_blocks_is_forked ON blocks USING btree (is_forked);
CREATE INDEX ix_blocks_miner ON blocks USING btree (miner);
CREATE INDEX ix_blocks_number ON blocks USING btree (number);


CREATE TABLE internal_transactions (
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    parent_tx_hash character varying NOT NULL,
    op character varying,
    call_depth integer,
    "timestamp" integer,
    "from" character varying,
    "to" character varying,
    value numeric(32,0),
    gas_limit bigint,
    payload character varying,
    status character varying,
    transaction_index integer NOT NULL,
    is_forked boolean default false
);
ALTER TABLE ONLY internal_transactions
    ADD CONSTRAINT internal_transactions_pkey PRIMARY KEY (block_hash, parent_tx_hash, transaction_index);
CREATE INDEX ix_internal_transactions_block_number ON internal_transactions USING btree (block_number);
CREATE INDEX ix_internal_transactions_is_forked ON internal_transactions USING btree (is_forked);


CREATE TABLE logs (
    transaction_hash character varying NOT NULL,
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    log_index integer NOT NULL,
    address character varying,
    data character varying,
    removed boolean default false,
    topics character varying[],
    transaction_index integer,
    event_type character varying,
    event_args jsonb,
    token_amount numeric,
    token_transfer_from character varying,
    token_transfer_to character varying,
    is_token_transfer boolean default false,
    is_processed boolean default false,
    is_forked boolean default false,
    is_transfer_processed boolean default false
);
ALTER TABLE ONLY logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (transaction_hash, block_hash, log_index);
CREATE INDEX ix_logs_block_hash ON logs USING btree (block_hash);
CREATE INDEX ix_logs_block_number ON logs USING btree (block_number);
CREATE INDEX ix_logs_is_forked ON logs USING btree (is_forked);
CREATE INDEX ix_logs_token_transfer_from ON logs USING btree (token_transfer_from);
CREATE INDEX ix_logs_token_transfer_to ON logs USING btree (token_transfer_to);
CREATE INDEX ix_logs_is_token_transfer_multicolumn ON logs 
    USING btree (is_token_transfer, is_transfer_processed, block_number);
CREATE INDEX ix_logs_is_transfer_processed_multicolumn ON logs 
    USING btree (is_processed, block_number);


CREATE TABLE receipts (
    transaction_hash character varying NOT NULL,
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    contract_address character varying,
    cumulative_gas_used integer,
    "from" character varying,
    "to" character varying,
    gas_used integer,
    logs_bloom character varying,
    root character varying,
    transaction_index integer NOT NULL,
    status integer,
    is_forked boolean default false
);
ALTER TABLE ONLY receipts
    ADD CONSTRAINT receipts_pkey PRIMARY KEY (block_hash, transaction_index);
CREATE INDEX ix_receipts_block_number ON receipts USING btree (block_number);
CREATE INDEX ix_receipts_contract_address ON receipts USING btree (contract_address);
CREATE INDEX ix_receipts_is_forked ON receipts USING btree (is_forked);
CREATE INDEX ix_receipts_transaction_hash ON receipts USING btree (transaction_hash);


CREATE TABLE reorgs (
    id bigint NOT NULL,
    block_hash character varying NOT NULL,
    block_number integer NOT NULL,
    reinserted boolean NOT NULL
);
ALTER TABLE ONLY reorgs
    ADD CONSTRAINT reorgs_pkey PRIMARY KEY (id);


CREATE TABLE token_holders (
    account_address character varying NOT NULL,
    token_address character varying NOT NULL,
    balance numeric,
    CONSTRAINT check_balance_positive CHECK ((balance >= (0)::numeric))
);
ALTER TABLE ONLY token_holders
    ADD CONSTRAINT token_holders_pkey PRIMARY KEY (account_address, token_address);
CREATE INDEX ix_token_holders_balance ON token_holders USING btree (balance);


CREATE TABLE transactions (
    hash character varying NOT NULL,
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    transaction_index integer NOT NULL,
    "from" character varying,
    "to" character varying,
    gas character varying,
    gas_price character varying,
    input character varying,
    nonce character varying,
    r character varying,
    s character varying,
    v character varying,
    value character varying,
    contract_call_description jsonb,
    is_forked boolean default false
);
ALTER TABLE ONLY transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (block_hash, transaction_index);
CREATE INDEX ix_transactions_block_hash ON transactions USING btree (block_hash);
CREATE INDEX ix_transactions_block_number ON transactions USING btree (block_number);
CREATE INDEX ix_transactions_from ON transactions USING btree ("from");
CREATE INDEX ix_transactions_hash ON transactions USING btree (hash);
CREATE INDEX ix_transactions_is_forked ON transactions USING btree (is_forked);
CREATE INDEX ix_transactions_to ON transactions USING btree ("to");


CREATE TABLE uncles (
    hash character varying NOT NULL,
    number integer NOT NULL,
    block_number integer NOT NULL,
    block_hash character varying NOT NULL,
    parent_hash character varying,
    difficulty bigint,
    extra_data character varying,
    gas_limit bigint,
    gas_used bigint,
    logs_bloom character varying,
    miner character varying,
    mix_hash character varying,
    nonce character varying,
    receipts_root character varying,
    sha3_uncles character varying,
    size integer,
    state_root character varying,
    "timestamp" integer,
    total_difficulty bigint,
    transactions_root character varying,
    reward numeric(32,0),
    is_forked boolean default false
);
ALTER TABLE ONLY uncles
    ADD CONSTRAINT uncles_pkey PRIMARY KEY (block_hash, hash);
CREATE INDEX ix_uncles_block_number ON uncles USING btree (block_number);
CREATE INDEX ix_uncles_is_forked ON uncles USING btree (is_forked);
CREATE INDEX ix_uncles_number ON uncles USING btree (number);
"""


DOWN_SQL = """
DROP TABLE accounts_base; 
DROP TABLE accounts_state; 
DROP TABLE blocks; 
DROP TABLE internal_transactions; 
DROP TABLE logs; 
DROP TABLE receipts; 
DROP TABLE reorgs; 
DROP TABLE token_holders; 
DROP TABLE transactions; 
DROP TABLE uncles; 
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
