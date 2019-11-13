-- +goose Up
-- +goose StatementBegin
DROP INDEX ix_blocks_timestamp;
CREATE INDEX ix_blocks_timestamp ON blocks USING btree (timestamp) WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 1;
-- +goose StatementEnd
