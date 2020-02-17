-- +goose Up
-- +goose NO TRANSACTION
create index concurrently if not exists token_holders_by_balance_id
    on token_holders (token_address, balance DESC, id DESC)
    where is_forked = false;

create index concurrently if not exists ix_token_holders_by_asset_address_and_block
    on token_holders using btree (token_address, account_address, block_number)
    where (is_forked = false);

-- +goose Down
drop index if exists token_holders_by_balance_id;
drop index if exists ix_token_holders_by_asset_address_and_block;
