"""
initial
"""

from yoyo import step

__depends__ = {}

steps = [
    step("""CREATE TABLE blocks (
                difficulty  bigint,
                extra_data   varchar,
                gas_limit    integer,
                gas_used integer,
                hash    varchar primary key,
                logs_bloom   varchar,
                miner   varchar,
                mix_hash varchar,
                nonce   varchar,
                number  integer,
                parent_hash  varchar,
                receipts_root    varchar,
                sha3_uncles  varchar,
                size    integer,
                state_root   varchar,
                timestamp   integer,
                total_difficulty bigint,
                transactions_root    varchar,
                is_sequence_sync boolean);
            """,
            'DROP TABLE blocks'),

    step("""CREATE TABLE uncles (
                difficulty  bigint,
                extra_data   varchar,
                gas_limit    integer,
                gas_used integer,
                hash    varchar primary key,
                logs_bloom   varchar,
                miner   varchar,
                mix_hash varchar,
                nonce   varchar,
                number  integer,
                parent_hash  varchar,
                receipts_root    varchar,
                sha3_uncles  varchar,
                size    integer,
                state_root   varchar,
                timestamp   integer,
                total_difficulty bigint,
                transactions_root    varchar,
                block_hash   varchar,
                block_number integer,
                FOREIGN KEY (block_hash) REFERENCES blocks (hash))
            """,
            'DROP TABLE uncles'),

    step("""CREATE TABLE transactions (
                block_hash   varchar,
                block_number integer,
                "from"    varchar,
                gas varchar,
                gas_price    varchar,
                hash    varchar,
                input   varchar,
                nonce   varchar,
                r   varchar,
                s   varchar,
                "to"  varchar,
                transaction_index    integer,
                v   varchar,
                value   varchar,
                FOREIGN KEY (block_hash) REFERENCES blocks (hash));
            """,
            'DROP TABLE transactions'),

    step("""CREATE TABLE receipts (
                block_hash   varchar,
                block_number integer,
                contract_address varchar,
                cumulative_gas_used   integer,
                "from"    varchar,
                gas_used integer,
                logs_bloom   varchar,
                root    varchar,
                "to"  varchar,
                transaction_hash varchar,
                transaction_index    integer,
                status integer,
                FOREIGN KEY (block_hash) REFERENCES blocks (hash));
            """,
            'DROP TABLE receipts'),

    step("""CREATE TABLE logs (
                address varchar,
                block_hash   varchar,
                block_number integer,
                data    varchar,
                log_index    integer,
                removed boolean,
                topics  varchar[],
                transaction_hash varchar,
                transaction_index    integer,
                FOREIGN KEY (block_hash) REFERENCES blocks (hash));
            """,
            'DROP TABLE logs'),

    step("""CREATE TABLE accounts (
                 block_number integer,
                 block_hash   varchar,
                 address varchar,
                 nonce   integer,
                 code    varchar,
                 code_hash    varchar,
                 root    varchar,
                 storage    jsonb,
                 balance NUMERIC(32, 0),
                FOREIGN KEY (block_hash) REFERENCES blocks (hash));
            """,
            'DROP TABLE accounts'),


]
