-- +goose Up
-- +goose StatementBegin
CREATE TABLE token_descriptions
(
    block_hash   character varying     NOT NULL,
    block_number bigint                NOT NULL,
    total_supply numeric,
    address      character varying     NOT NULL,
    is_forked    boolean DEFAULT false NOT NULL
);
CREATE INDEX ix_token_desc_by_hash on token_descriptions (block_hash);
CREATE INDEX ix_token_desc_by_token on token_descriptions (address, block_number) where is_forked = false;

GRANT SELECT on token_descriptions TO jsearch_main_ro_user;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE token_descriptions;
-- +goose StatementEnd
