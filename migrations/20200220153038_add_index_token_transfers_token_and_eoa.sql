-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_token_transfers_token_address_address_keyset_partial ON token_transfers USING btree (token_address, address, block_number, transaction_index, log_index) WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
DROP INDEX CONCURRENTLY IF EXISTS ix_token_transfers_token_address_address_keyset_partial;
-- +goose StatementEnd
