
CREATE TABLE public.accounts (
    id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    address character varying(45) NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.accounts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.accounts_id_seq OWNED BY public.accounts.id;



CREATE TABLE public.bodies (
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE public.chain_events (
    id bigint NOT NULL,
    block_number bigint,
    block_hash character varying(70),
    parent_block_hash character varying(70),
    type character varying(20) NOT NULL,
    common_block_number bigint NOT NULL,
    common_block_hash character varying(70) NOT NULL,
    drop_length bigint NOT NULL,
    drop_block_hash character varying(70) NOT NULL,
    add_length bigint NOT NULL,
    add_block_hash character varying(70) NOT NULL,
    node_id character varying(70),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.chain_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.chain_events_id_seq OWNED BY public.chain_events.id;



CREATE TABLE public.chain_splits (
    id bigint NOT NULL,
    common_block_number bigint NOT NULL,
    common_block_hash character varying(70) NOT NULL,
    drop_length bigint NOT NULL,
    drop_block_hash character varying(70) NOT NULL,
    add_length bigint NOT NULL,
    add_block_hash character varying(70) NOT NULL,
    node_id character varying(70),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.chain_splits_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.chain_splits_id_seq OWNED BY public.chain_splits.id;



CREATE TABLE public.goose_db_version (
    id integer NOT NULL,
    version_id bigint NOT NULL,
    is_applied boolean NOT NULL,
    tstamp timestamp without time zone DEFAULT now()
);


CREATE SEQUENCE public.goose_db_version_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.goose_db_version_id_seq OWNED BY public.goose_db_version.id;



CREATE TABLE public.headers (
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE public.internal_transactions (
    id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    parent_tx_hash character varying(70) NOT NULL,
    index bigint NOT NULL,
    type character varying(20) NOT NULL,
    "timestamp" bigint NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    call_depth integer DEFAULT 0
);


CREATE SEQUENCE public.internal_transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.internal_transactions_id_seq OWNED BY public.internal_transactions.id;


CREATE TABLE public.pending_transactions (
    tx_hash character varying(70) NOT NULL,
    status character varying,
    fields jsonb,
    id bigint NOT NULL,
    "timestamp" timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    removed boolean DEFAULT false,
    node_id character varying(70),
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.pending_transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.pending_transactions_id_seq OWNED BY public.pending_transactions.id;



CREATE TABLE public.receipts (
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE public.reorgs (
    id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    header jsonb,
    reinserted boolean NOT NULL,
    node_id character varying(70),
    split_id integer,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.reorgs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.reorgs_id_seq OWNED BY public.reorgs.id;



CREATE TABLE public.rewards (
    id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    address character varying(45) NOT NULL,
    fields jsonb,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE SEQUENCE public.rewards_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.rewards_id_seq OWNED BY public.rewards.id;



CREATE TABLE public.token_holders (
    id bigint NOT NULL,
    block_number bigint NOT NULL,
    block_hash character varying(70) NOT NULL,
    token_address character varying(45) NOT NULL,
    holder_address character varying(45) NOT NULL,
    balance numeric NOT NULL,
    decimals smallint
);


CREATE SEQUENCE public.token_holders_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.token_holders_id_seq OWNED BY public.token_holders.id;



ALTER TABLE ONLY public.accounts ALTER COLUMN id SET DEFAULT nextval('public.accounts_id_seq'::regclass);



ALTER TABLE ONLY public.chain_events ALTER COLUMN id SET DEFAULT nextval('public.chain_events_id_seq'::regclass);



ALTER TABLE ONLY public.chain_splits ALTER COLUMN id SET DEFAULT nextval('public.chain_splits_id_seq'::regclass);



ALTER TABLE ONLY public.goose_db_version ALTER COLUMN id SET DEFAULT nextval('public.goose_db_version_id_seq'::regclass);



ALTER TABLE ONLY public.internal_transactions ALTER COLUMN id SET DEFAULT nextval('public.internal_transactions_id_seq'::regclass);



ALTER TABLE ONLY public.pending_transactions ALTER COLUMN id SET DEFAULT nextval('public.pending_transactions_id_seq'::regclass);



ALTER TABLE ONLY public.reorgs ALTER COLUMN id SET DEFAULT nextval('public.reorgs_id_seq'::regclass);



ALTER TABLE ONLY public.rewards ALTER COLUMN id SET DEFAULT nextval('public.rewards_id_seq'::regclass);



ALTER TABLE ONLY public.token_holders ALTER COLUMN id SET DEFAULT nextval('public.token_holders_id_seq'::regclass);



ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_block_hash_address_key UNIQUE (block_hash, address);



ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.bodies
    ADD CONSTRAINT bodies_block_hash_key UNIQUE (block_hash);



ALTER TABLE ONLY public.chain_events
    ADD CONSTRAINT chain_events_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.chain_splits
    ADD CONSTRAINT chain_splits_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.goose_db_version
    ADD CONSTRAINT goose_db_version_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.headers
    ADD CONSTRAINT headers_block_hash_key UNIQUE (block_hash);



ALTER TABLE ONLY public.internal_transactions
    ADD CONSTRAINT internal_transactions_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.pending_transactions
    ADD CONSTRAINT pending_transactions_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.receipts
    ADD CONSTRAINT receipts_block_hash_key UNIQUE (block_hash);



ALTER TABLE ONLY public.reorgs
    ADD CONSTRAINT reorgs_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.rewards
    ADD CONSTRAINT rewards_block_hash_key UNIQUE (block_hash);



ALTER TABLE ONLY public.rewards
    ADD CONSTRAINT rewards_pkey PRIMARY KEY (id);



ALTER TABLE ONLY public.token_holders
    ADD CONSTRAINT token_holders_block_hash_token_address_holder_address_key UNIQUE (block_hash, token_address, holder_address);



ALTER TABLE ONLY public.token_holders
    ADD CONSTRAINT token_holders_pkey PRIMARY KEY (id);



CREATE INDEX accounts_block_hash_idx ON public.accounts USING btree (block_hash);



CREATE INDEX accounts_block_number_idx ON public.accounts USING btree (block_number);



CREATE INDEX bodies_block_hash_idx ON public.bodies USING btree (block_hash);



CREATE INDEX bodies_block_number_idx ON public.bodies USING btree (block_number);



CREATE INDEX chain_events_block_hash_idx ON public.chain_events USING btree (block_hash);



CREATE INDEX chain_events_block_number_and_id_idx ON public.chain_events USING btree (block_number, id);



CREATE INDEX chain_events_block_number_idx ON public.chain_events USING btree (block_number);



CREATE INDEX chain_events_common_block_hash_idx ON public.chain_events USING btree (common_block_hash);



CREATE INDEX chain_events_common_block_number_idx ON public.chain_events USING btree (common_block_number);



CREATE INDEX chain_events_node_id_idx ON public.chain_events USING btree (node_id);



CREATE INDEX chain_events_parent_block_hash_idx ON public.chain_events USING btree (parent_block_hash);



CREATE INDEX chain_events_type_idx ON public.chain_events USING btree (type);



CREATE INDEX chain_splits_common_block_number_idx ON public.chain_splits USING btree (common_block_number);



CREATE INDEX headers_block_hash_idx ON public.headers USING btree (block_hash);



CREATE INDEX headers_block_number_idx ON public.headers USING btree (block_number);



CREATE INDEX internal_transactions_block_hash_idx ON public.internal_transactions USING btree (block_hash);



CREATE INDEX internal_transactions_block_number_idx ON public.internal_transactions USING btree (block_number);



CREATE INDEX internal_transactions_type_idx ON public.internal_transactions USING btree (type);



CREATE UNIQUE INDEX internal_transactions_uniq_2 ON public.internal_transactions USING btree (block_hash, parent_tx_hash, index, type, call_depth);



CREATE INDEX pending_transactions_status_idx ON public.pending_transactions USING btree (status);



CREATE INDEX pending_transactions_tx_hash_idx ON public.pending_transactions USING btree (tx_hash);



CREATE INDEX receipts_block_hash_idx ON public.receipts USING btree (block_hash);



CREATE INDEX receipts_block_number_idx ON public.receipts USING btree (block_number);



CREATE INDEX reorgs_block_hash_idx ON public.reorgs USING btree (block_hash);



CREATE INDEX reorgs_block_number_idx ON public.reorgs USING btree (block_number);



CREATE INDEX reorgs_reinserted_idx ON public.reorgs USING btree (reinserted);



CREATE INDEX reorgs_split_id_idx ON public.reorgs USING btree (split_id);



CREATE INDEX rewards_block_hash_idx ON public.rewards USING btree (block_hash);



CREATE INDEX rewards_block_number_idx ON public.rewards USING btree (block_number);


