-- +goose NO TRANSACTION
-- +goose Up
-- +goose StatementBegin
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_asset_summary_address_asset_address_block_number_partial ON assets_summary USING btree (address, asset_address, block_number) WHERE (is_forked = false);
-- +goose StatementEnd

-- +goose NO TRANSACTION
-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS ix_asset_summary_address_asset_address_block_number_partial;
-- +goose StatementEnd
