-- +goose Up
-- +goose StatementBegin

--
-- PostgreSQL database dump
--

-- Dumped from database version 11.2 (Debian 11.2-1.pgdg90+1)
-- Dumped by pg_dump version 11.2 (Debian 11.2-1.pgdg90+1)

--
-- Name: check_canonical_chain(integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION check_canonical_chain(depth integer)
    RETURNS TABLE
            (
                number      bigint,
                hash        character varying,
                parent_hash character varying
            )
    LANGUAGE plpgsql
    STABLE
AS
$$
DECLARE
    last_hash varchar;
BEGIN

    FOR number, hash, parent_hash IN
        SELECT b.number, b.hash, b.parent_hash
        FROM blocks b
        WHERE b.is_forked = false
          AND b.number > ((SELECT MAX(bb.number) FROM blocks bb) - depth)
        ORDER BY b.number ASC
        LOOP
            IF last_hash <> parent_hash THEN
                RETURN NEXT;
            END IF;
            last_hash := hash;
        END LOOP;

END
$$;


--
-- Name: clean_holder(character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION clean_holder(token character varying) RETURNS integer
    LANGUAGE plpgsql
AS
$$
DECLARE
    last_hash varchar;
    acc       RECORD;
BEGIN

    FOR acc IN
        select distinct account_address from token_holders where token_address = token
        LOOP
            delete
            from token_holders h
            where h.token_address = token
              and h.account_address = acc.account_address
              and h.block_number < (select max(block_number) - 6
                                    from token_holders hh
                                    where hh.token_address = token
                                      and hh.account_address = acc.account_address
                                      and hh.is_forked = false);
        END LOOP;
    RETURN 1;
END
$$;



--
-- Name: insert_block_data(text, text, text, text, text, text, text, text, text, text, text, text, text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE OR REPLACE FUNCTION insert_block_data(block_data text, uncles_data text, transactions_data text, receipts_data text,
                                  logs_data text, accounts_state_data text, accounts_base_data text,
                                  internal_txs_data text, transfers_data text, token_holders_updates_data text,
                                  wallet_events_data text, assets_summary_updates_data text,
                                  chain_event_data text) RETURNS boolean
    LANGUAGE plpgsql
AS
$$
BEGIN
    INSERT INTO blocks SELECT * FROM json_populate_recordset(null::blocks, block_data::json);

    IF json_array_length(uncles_data::json) > 0 THEN
        INSERT INTO uncles SELECT * FROM json_populate_recordset(null::uncles, uncles_data::json);
    END IF;

    IF json_array_length(transactions_data::json) > 0 THEN
        INSERT INTO transactions SELECT * FROM json_populate_recordset(null::transactions, transactions_data::json);
    END IF;

    IF json_array_length(receipts_data::json) > 0 THEN
        INSERT INTO receipts SELECT * FROM json_populate_recordset(null::receipts, receipts_data::json);
    END IF;

    IF json_array_length(logs_data::json) > 0 THEN
        INSERT INTO logs SELECT * FROM json_populate_recordset(null::logs, logs_data::json);
    END IF;

    IF json_array_length(accounts_state_data::json) > 0 THEN
        INSERT INTO accounts_state
        SELECT *
        FROM json_populate_recordset(null::accounts_state, accounts_state_data::json);
    END IF;

    IF json_array_length(accounts_base_data::json) > 0 THEN
        INSERT INTO accounts_base SELECT * FROM json_populate_recordset(null::accounts_base, accounts_base_data::json);
    END IF;

    IF json_array_length(internal_txs_data::json) > 0 THEN
        INSERT INTO internal_transactions
        SELECT *
        FROM json_populate_recordset(null::internal_transactions, internal_txs_data::json);
    END IF;

    IF json_array_length(transfers_data::json) > 0 THEN
        INSERT INTO token_transfers SELECT * FROM json_populate_recordset(null::token_transfers, transfers_data::json);
    END IF;

    /*
        `SELECT account_address, token_address, balance, decimals, block_number` -
        need to prevent insert null to `id` field which will cause error.
    */
    IF json_array_length(token_holders_updates_data::json) > 0 THEN
        INSERT INTO token_holders (account_address, token_address, balance, decimals, block_number, block_hash,
                                   is_forked)
        SELECT account_address,
               token_address,
               balance,
               decimals,
               block_number,
               block_hash,
               CASE WHEN is_forked is null THEN false ELSE is_forked END as is_forked
        FROM json_populate_recordset(null::token_holders, token_holders_updates_data::json)
        ON CONFLICT (account_address, token_address, block_hash)
            DO UPDATE SET balance = EXCLUDED.balance, block_number = EXCLUDED.block_number
        WHERE token_holders.block_number is null
           or token_holders.block_number < EXCLUDED.block_number;
    END IF;

    IF json_array_length(wallet_events_data::json) > 0 THEN
        INSERT INTO wallet_events SELECT * FROM json_populate_recordset(null::wallet_events, wallet_events_data::json);
    END IF;

    IF json_array_length(assets_summary_updates_data::json) > 0 THEN
        INSERT INTO assets_summary (address, asset_address, "value", decimals, nonce, tx_number, block_number,
                                    block_hash, is_forked)
        SELECT address,
               asset_address,
               "value",
               decimals,
               nonce,
               tx_number,
               block_number,
               block_hash,
               CASE WHEN is_forked is null THEN false ELSE is_forked END as is_forked
        FROM json_populate_recordset(null::assets_summary, assets_summary_updates_data::json)
        ON CONFLICT (address, asset_address, block_hash)
            DO UPDATE SET value        = EXCLUDED.value,
                          block_number = EXCLUDED.block_number,
                          nonce        = EXCLUDED.nonce,
                          tx_number    = EXCLUDED.tx_number
        WHERE assets_summary.block_number is null
           or assets_summary.block_number < EXCLUDED.block_number;
    END IF;

    IF char_length(chain_event_data) > 2 THEN
        INSERT INTO chain_events SELECT * FROM json_populate_record(null::chain_events, chain_event_data::json);
    END IF;

    RETURN true;
END;
$$;


--
-- Name: row_estimator(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION row_estimator(query text) RETURNS bigint
    LANGUAGE plpgsql
AS
$$
DECLARE
    plan jsonb;
BEGIN
    EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;

    RETURN (plan -> 0 -> 'Plan' ->> 'Plan Rows')::bigint;
END;
$$;


--
-- Name: accounts_base; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE accounts_base
(
    address            character varying NOT NULL,
    code               character varying,
    code_hash          character varying,
    last_known_balance numeric(32, 0),
    root               character varying
);



--
-- Name: accounts_state; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE accounts_state
(
    block_number integer           NOT NULL,
    block_hash   character varying NOT NULL,
    address      character varying NOT NULL,
    nonce        integer,
    root         character varying,
    balance      numeric(32, 0),
    is_forked    boolean DEFAULT false
);

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: postgres
--

--
-- Name: assets_summary; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE assets_summary
(
    address       character varying     NOT NULL,
    asset_address character varying     NOT NULL,
    tx_number     integer,
    nonce         integer,
    value         numeric,
    decimals      integer,
    block_number  integer,
    is_forked     boolean DEFAULT false NOT NULL,
    block_hash    character varying
);

--
-- Name: blocks; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE blocks
(
    number                 integer           NOT NULL,
    hash                   character varying NOT NULL,
    parent_hash            character varying,
    difficulty             bigint,
    extra_data             character varying,
    gas_limit              bigint,
    gas_used               bigint,
    logs_bloom             character varying,
    miner                  character varying,
    mix_hash               character varying,
    nonce                  character varying,
    receipts_root          character varying,
    sha3_uncles            character varying,
    size                   integer,
    state_root             character varying,
    "timestamp"            integer,
    total_difficulty       bigint,
    transactions_root      character varying,
    is_sequence_sync       boolean DEFAULT false,
    static_reward          numeric(32, 0),
    uncle_inclusion_reward numeric(32, 0),
    tx_fees                numeric(32, 0),
    is_forked              boolean DEFAULT false,
    transactions           jsonb,
    uncles                 jsonb
);


--
-- Name: chain_events; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE chain_events
(
    id                  bigint NOT NULL,
    block_hash          character varying,
    block_number        bigint,
    type                character varying,
    parent_block_hash   character varying,
    common_block_number bigint,
    common_block_hash   character varying,
    drop_length         bigint,
    drop_block_hash     character varying,
    add_length          bigint,
    add_block_hash      character varying,
    node_id             character varying,
    created_at          timestamp without time zone
);


-- Name: internal_transactions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE internal_transactions
(
    block_number      integer           NOT NULL,
    block_hash        character varying NOT NULL,
    parent_tx_hash    character varying NOT NULL,
    op                character varying,
    call_depth        integer,
    "timestamp"       integer,
    "from"            character varying,
    "to"              character varying,
    value             numeric(32, 0),
    gas_limit         bigint,
    payload           character varying,
    status            character varying,
    transaction_index integer           NOT NULL,
    is_forked         boolean DEFAULT false,
    tx_origin         character varying,
    parent_tx_index   integer
);


--
-- Name: logs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE logs
(
    transaction_hash      character varying NOT NULL,
    block_number          integer           NOT NULL,
    block_hash            character varying NOT NULL,
    log_index             integer           NOT NULL,
    address               character varying,
    data                  character varying,
    removed               boolean DEFAULT false,
    topics                character varying[],
    transaction_index     integer,
    event_type            character varying,
    event_args            jsonb,
    token_amount          numeric,
    token_transfer_from   character varying,
    token_transfer_to     character varying,
    is_token_transfer     boolean DEFAULT false,
    is_processed          boolean DEFAULT false,
    is_forked             boolean DEFAULT false,
    is_transfer_processed boolean DEFAULT false,
    "timestamp"           integer
);

--
-- Name: pending_transactions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE pending_transactions
(
    last_synced_id bigint                      NOT NULL,
    hash           character varying(70)       NOT NULL,
    status         character varying,
    "timestamp"    timestamp without time zone NOT NULL,
    removed        boolean                     NOT NULL,
    node_id        character varying(70),
    r              character varying,
    s              character varying,
    v              character varying,
    "to"           character varying,
    "from"         character varying,
    gas            bigint,
    gas_price      bigint,
    input          character varying,
    nonce          bigint,
    value          character varying,
    id             integer                     NOT NULL
);


--
-- Name: pending_transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE pending_transactions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: pending_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE pending_transactions_id_seq OWNED BY pending_transactions.id;


--
-- Name: receipts; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE receipts
(
    transaction_hash    character varying NOT NULL,
    block_number        integer           NOT NULL,
    block_hash          character varying NOT NULL,
    contract_address    character varying,
    cumulative_gas_used integer,
    "from"              character varying,
    "to"                character varying,
    gas_used            integer,
    logs_bloom          character varying,
    root                character varying,
    transaction_index   integer           NOT NULL,
    status              integer,
    is_forked           boolean DEFAULT false
);

--
-- Name: reorgs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE reorgs
(
    block_hash   character varying     NOT NULL,
    block_number integer               NOT NULL,
    reinserted   boolean               NOT NULL,
    node_id      character varying(70) NOT NULL,
    split_id     bigint,
    id           bigint                NOT NULL
);

--
-- Name: reorgs_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE reorgs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

--
-- Name: reorgs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE reorgs_id_seq OWNED BY reorgs.id;


--
-- Name: token_holders; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE token_holders
(
    account_address character varying     NOT NULL,
    token_address   character varying     NOT NULL,
    balance         numeric,
    decimals        integer,
    block_number    integer,
    id              bigint                NOT NULL,
    block_hash      character varying,
    is_forked       boolean DEFAULT false NOT NULL,
    CONSTRAINT check_balance_positive CHECK ((balance >= (0)::numeric))
);

--
-- Name: token_holders_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE token_holders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

-- Name: token_holders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE token_holders_id_seq OWNED BY token_holders.id;


--
-- Name: token_transfers; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE token_transfers
(
    address           character varying,
    transaction_hash  character varying,
    log_index         integer,
    block_number      integer,
    block_hash        character varying NOT NULL,
    "timestamp"       integer,
    from_address      character varying,
    to_address        character varying,
    token_address     character varying,
    token_value       numeric,
    token_decimals    integer,
    token_name        character varying,
    token_symbol      character varying,
    is_forked         boolean DEFAULT false,
    transaction_index integer,
    status            integer
);

--
-- Name: transactions; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE transactions
(
    hash                      character varying NOT NULL,
    block_number              integer           NOT NULL,
    block_hash                character varying NOT NULL,
    transaction_index         integer           NOT NULL,
    "from"                    character varying,
    "to"                      character varying,
    gas                       character varying,
    gas_price                 character varying,
    input                     character varying,
    nonce                     character varying,
    r                         character varying,
    s                         character varying,
    v                         character varying,
    value                     character varying,
    contract_call_description jsonb,
    is_forked                 boolean           DEFAULT false,
    address                   character varying DEFAULT ''::character varying,
    status                    integer,
    "timestamp"               integer
);


--
-- Name: uncles; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE uncles
(
    hash              character varying NOT NULL,
    number            integer           NOT NULL,
    block_number      integer           NOT NULL,
    block_hash        character varying NOT NULL,
    parent_hash       character varying,
    difficulty        bigint,
    extra_data        character varying,
    gas_limit         bigint,
    gas_used          bigint,
    logs_bloom        character varying,
    miner             character varying,
    mix_hash          character varying,
    nonce             character varying,
    receipts_root     character varying,
    sha3_uncles       character varying,
    size              integer,
    state_root        character varying,
    "timestamp"       integer,
    total_difficulty  bigint,
    transactions_root character varying,
    reward            numeric(32, 0),
    is_forked         boolean DEFAULT false
);


--
-- Name: wallet_events; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE wallet_events
(
    address      character varying,
    type         character varying,
    tx_hash      character varying,
    block_hash   character varying,
    block_number bigint,
    event_index  bigint,
    is_forked    boolean DEFAULT false,
    tx_data      json,
    event_data   json
);


--
-- Name: pending_transactions id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY pending_transactions
    ALTER COLUMN id SET DEFAULT nextval('pending_transactions_id_seq'::regclass);


--
-- Name: reorgs id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY reorgs
    ALTER COLUMN id SET DEFAULT nextval('reorgs_id_seq'::regclass);


--
-- Name: token_holders id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY token_holders
    ALTER COLUMN id SET DEFAULT nextval('token_holders_id_seq'::regclass);


--
-- Name: accounts_state accounts_state_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY accounts_state
    ADD CONSTRAINT accounts_state_pkey PRIMARY KEY (block_hash, address);

--
-- Name: blocks blocks_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY blocks
    ADD CONSTRAINT blocks_pkey PRIMARY KEY (hash);


--
-- Name: chain_events chain_events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY chain_events
    ADD CONSTRAINT chain_events_pkey PRIMARY KEY (id);


--
-- Name: logs logs_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY logs
    ADD CONSTRAINT logs_pkey PRIMARY KEY (transaction_hash, block_hash, log_index);


--
-- Name: pending_transactions pending_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY pending_transactions
    ADD CONSTRAINT pending_transactions_pkey PRIMARY KEY (hash);


--
-- Name: receipts receipts_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY receipts
    ADD CONSTRAINT receipts_pkey PRIMARY KEY (block_hash, transaction_index);


--
-- Name: uncles uncles_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY uncles
    ADD CONSTRAINT uncles_pkey PRIMARY KEY (block_hash, hash);


--
-- Name: assets_summary_by_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX assets_summary_by_block_hash ON assets_summary USING btree (block_hash, asset_address, address);


--
-- Name: ix_accounts_base_address; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_accounts_base_address ON accounts_base USING btree (address);


--
-- Name: ix_accounts_state_address_block_number_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_accounts_state_address_block_number_partial ON accounts_state USING btree (address, block_number) WHERE (is_forked = false);


--
-- Name: ix_accounts_state_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_accounts_state_block_number ON accounts_state USING btree (block_number);


--
-- Name: ix_asset_summary_address_asset_block_number_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_asset_summary_address_asset_block_number_partial ON assets_summary USING btree (address, block_number, asset_address) WHERE (is_forked = false);


--
-- Name: ix_blocks_miner; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_blocks_miner ON blocks USING btree (miner);


--
-- Name: ix_blocks_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_blocks_number ON blocks USING btree (number) WHERE (is_forked = false);


--
-- Name: ix_blocks_timestamp; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_blocks_timestamp ON blocks USING btree (number) WHERE (is_forked = false);


--
-- Name: ix_chain_events_by_block_node_and_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_chain_events_by_block_node_and_id ON chain_events USING btree (id, block_number);


--
-- Name: ix_internal_transactions_block_hash_parent_tx_hash_transaction_; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_internal_transactions_block_hash_parent_tx_hash_transaction_ ON internal_transactions USING btree (block_hash, parent_tx_hash, transaction_index);


--
-- Name: ix_internal_transactions_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_internal_transactions_block_number ON internal_transactions USING btree (block_number);


--
-- Name: ix_internal_transactions_parent_tx_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_internal_transactions_parent_tx_hash ON internal_transactions USING btree (parent_tx_hash);


--
-- Name: ix_internal_transactions_tx_origin; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_internal_transactions_tx_origin ON internal_transactions USING btree (tx_origin) WHERE (is_forked = false);


--
-- Name: ix_logs_address; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_address ON logs USING btree (address);


--
-- Name: ix_logs_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_block_hash ON logs USING btree (block_hash);


--
-- Name: ix_logs_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_block_number ON logs USING btree (block_number);


--
-- Name: ix_logs_keyset_by_block; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_keyset_by_block ON logs USING btree (address, block_number, transaction_index, log_index) WHERE (is_forked = false);


--
-- Name: ix_logs_keyset_by_timestamp; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_keyset_by_timestamp ON logs USING btree (address, "timestamp", transaction_index, log_index) WHERE (is_forked = false);


--
-- Name: ix_logs_token_transfer_from; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_token_transfer_from ON logs USING btree (token_transfer_from);


--
-- Name: ix_logs_token_transfer_to; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_logs_token_transfer_to ON logs USING btree (token_transfer_to);


--
-- Name: ix_pending_transactions_last_synced_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_pending_transactions_last_synced_id ON pending_transactions USING btree (last_synced_id);


--
-- Name: ix_pending_txs_from_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_pending_txs_from_partial ON pending_transactions USING btree ("from", "timestamp", id) WHERE (removed = false);


--
-- Name: ix_pending_txs_to_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_pending_txs_to_partial ON pending_transactions USING btree ("to", "timestamp", id) WHERE (removed = false);


--
-- Name: ix_receipts_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_receipts_block_number ON receipts USING btree (block_number);


--
-- Name: ix_receipts_contract_address; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_receipts_contract_address ON receipts USING btree (contract_address);


--
-- Name: ix_receipts_transaction_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_receipts_transaction_hash ON receipts USING btree (transaction_hash);


--
-- Name: ix_reorgs_hash_split_id_node_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX ix_reorgs_hash_split_id_node_id ON reorgs USING btree (block_hash, split_id, node_id);


--
-- Name: ix_reorgs_split_id; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_reorgs_split_id ON reorgs USING btree (split_id);


--
-- Name: ix_token_holders_by_token_block_number_and_address; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_token_holders_by_token_block_number_and_address ON token_holders USING btree (token_address, block_number, account_address) WHERE (is_forked = false);


--
-- Name: ix_token_transfers_address_block_number_log_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_token_transfers_address_block_number_log_index ON token_transfers USING btree (address, block_number, transaction_index, log_index);


--
-- Name: ix_token_transfers_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_token_transfers_block_hash ON token_transfers USING btree (block_hash);


--
-- Name: ix_token_transfers_token_address_block_number_log_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_token_transfers_token_address_block_number_log_index ON token_transfers USING btree (token_address, block_number, transaction_index, log_index);


--
-- Name: ix_transactions_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_block_hash ON transactions USING btree (block_hash);


--
-- Name: ix_transactions_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_block_number ON transactions USING btree (block_number);


--
-- Name: ix_transactions_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_hash ON transactions USING btree (hash);


--
-- Name: ix_transactions_keyset_by_block; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_keyset_by_block ON internal_transactions USING btree (tx_origin, block_number, parent_tx_index, transaction_index) WHERE (is_forked = false);


--
-- Name: ix_transactions_keyset_by_timestamp; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_keyset_by_timestamp ON internal_transactions USING btree (tx_origin, "timestamp", parent_tx_index, transaction_index) WHERE (is_forked = false);


--
-- Name: ix_transactions_timestamp_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_transactions_timestamp_index ON transactions USING btree (address, "timestamp", transaction_index) WHERE (is_forked = false);


--
-- Name: ix_uncles_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_uncles_block_hash ON uncles USING btree (block_hash);


--
-- Name: ix_uncles_block_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_uncles_block_number ON uncles USING btree (block_number);



--
-- Name: ix_uncles_number; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_uncles_number ON uncles USING btree (number);


--
-- Name: ix_wallet_events_address_event_index_type_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_wallet_events_address_event_index_type_partial ON wallet_events USING btree (address, event_index, type) WHERE (is_forked = false);


--
-- Name: ix_wallet_events_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX ix_wallet_events_block_hash ON wallet_events USING btree (block_hash);


--
-- Name: token_holders_by_block_hash; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX token_holders_by_block_hash ON token_holders USING btree (block_hash, token_address, account_address);


--
-- Name: transactions_address_idx_partial; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX transactions_address_idx_partial ON transactions USING btree (address, block_number, transaction_index) WHERE (is_forked = false);


--
-- PostgreSQL database dump complete
--

-- +goose StatementEnd

-- +goose Down
DROP FUNCTION check_canonical_chain;
DROP FUNCTION clean_holder;
DROP FUNCTION insert_block_data;
DROP FUNCTION row_estimator;

DROP TABLE accounts_base;
DROP TABLE accounts_state;
DROP TABLE assets_summary;
DROP TABLE wallet_events;
DROP TABLE blocks;
DROP TABLE transactions;
DROP TABLE internal_transactions;
DROP TABLE chain_events;
DROP TABLE reorgs;
DROP TABLE uncles;
DROP TABLE receipts;
DROP TABLE token_transfers;
DROP TABLE token_holders;
DROP TABLE pending_transactions;
DROP TABLE logs;

-- +goose StatementBegin
-- +goose StatementEnd
