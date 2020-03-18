-- +goose NO TRANSACTION
-- +goose Up
CREATE INDEX CONCURRENTLY IF NOT EXISTS token_holders_by_balance_id ON token_holders
    USING btree (token_address, balance DESC, id DESC) WHERE (is_forked = false);

CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_token_holders_by_asset_address_and_block ON token_holders
    USING btree (token_address, account_address, block_number) WHERE (is_forked = false);

DROP INDEX IF EXISTS ix_token_holders_by_token_block_number_and_address;

-- +goose NO TRANSACTION
-- +goose Down
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_token_holders_by_token_block_number_and_address ON token_holders
    USING btree (token_address, block_number, account_address) WHERE (is_forked = false);

DROP INDEX IF EXISTS token_holders_by_balance_id;
DROP INDEX IF EXISTS ix_token_holders_by_asset_address_and_block;
