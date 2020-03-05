-- +goose Up
-- +goose StatementBegin
--
-- reorgs
--

ALTER SEQUENCE reorgs_id_seq RENAME TO reorgs_id_old_seq;

CREATE SEQUENCE reorgs_id_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE reorgs_id_seq OWNED BY reorgs.id;

ALTER TABLE ONLY reorgs
    ALTER COLUMN id SET DEFAULT nextval('reorgs_id_seq'::regclass);

SELECT setval('reorgs_id_seq', (SELECT last_value FROM reorgs_id_old_seq), true);

DROP SEQUENCE reorgs_id_old_seq;

--
-- token_holders
--

ALTER SEQUENCE token_holders_id_seq RENAME TO token_holders_id_old_seq;

CREATE SEQUENCE token_holders_id_seq
    AS bigint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE token_holders_id_seq OWNED BY token_holders.id;

ALTER TABLE ONLY token_holders
    ALTER COLUMN id SET DEFAULT nextval('token_holders_id_seq'::regclass);

SELECT setval('token_holders_id_seq', (SELECT last_value FROM token_holders_id_old_seq), true);

DROP SEQUENCE token_holders_id_old_seq;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
--
-- reorgs
--

ALTER SEQUENCE reorgs_id_seq RENAME TO reorgs_id_old_seq;

CREATE SEQUENCE reorgs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE reorgs_id_seq OWNED BY reorgs.id;

ALTER TABLE ONLY reorgs
    ALTER COLUMN id SET DEFAULT nextval('reorgs_id_seq'::regclass);

SELECT setval('reorgs_id_seq', (SELECT last_value FROM reorgs_id_old_seq), true);

DROP SEQUENCE reorgs_id_old_seq;

--
-- token_holders
--

ALTER SEQUENCE token_holders_id_seq RENAME TO token_holders_id_old_seq;

CREATE SEQUENCE token_holders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE token_holders_id_seq OWNED BY token_holders.id;

ALTER TABLE ONLY token_holders
    ALTER COLUMN id SET DEFAULT nextval('token_holders_id_seq'::regclass);

SELECT setval('token_holders_id_seq', (SELECT last_value FROM token_holders_id_old_seq), true);

DROP SEQUENCE token_holders_id_old_seq;
-- +goose StatementEnd
