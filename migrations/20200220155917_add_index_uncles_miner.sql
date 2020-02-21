-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_uncles_miner ON uncles USING btree (miner);
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
DROP INDEX CONCURRENTLY IF EXISTS ix_uncles_miner;
-- +goose StatementEnd
