-- +goose Up
-- +goose StatementBegin
CREATE TABLE dex_logs
(
    block_hash   character varying NOT NULL,
    block_number bigint            NOT NULL,
    "timestamp"  integer           NOT NULL,
    tx_hash      character varying NOT NULL,
    is_forked    boolean DEFAULT false,

    event_type   character varying,
    event_data   jsonb
);


CREATE INDEX ix_dex_logs_by_timestamp ON dex_logs (timestamp) WHERE is_forked = FALSE;
CREATE INDEX ix_dex_logs_by_block_number ON dex_logs (block_number) WHERE is_forked = FALSE;
CREATE INDEX ix_dex_logs_by_block_hash ON dex_logs USING HASH (block_hash);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE dex_logs;
-- +goose StatementEnd
