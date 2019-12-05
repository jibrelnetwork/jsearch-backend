-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_to_partial ON internal_transactions USING btree ("to", "block_number", "timestamp") WHERE (is_forked = false);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_from_partial ON internal_transactions USING btree ("from", "block_number", "timestamp") WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_to_partial;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_from_partial;
-- +goose StatementEnd
