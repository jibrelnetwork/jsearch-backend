-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_asset_summary_address_asset_block_number_partial;
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_asset_summary_address_asset_block_number_partial ON assets_summary USING btree (address, block_number, asset_address) WHERE (is_forked = false);
-- +goose StatementEnd
