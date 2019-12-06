-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_to_partial_block_number ON internal_transactions USING btree ("to", "block_number", "parent_tx_index", "transaction_index") WHERE (is_forked = false);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_from_partial_block_number ON internal_transactions USING btree ("from", "block_number", "parent_tx_index", "transaction_index") WHERE (is_forked = false);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_to_partial_timestamp ON internal_transactions USING btree ("to", "timestamp", "parent_tx_index", "transaction_index") WHERE (is_forked = false);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_internal_transactions_from_partial_timestamp ON internal_transactions USING btree ("from", "timestamp", "parent_tx_index", "transaction_index") WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_to_partial_block_number;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_from_partial_block_number;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_to_partial_timestamp;
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_internal_transactions_from_partial_timestamp;
-- +goose StatementEnd
