-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION delete_block(number integer, _hash character varying)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
BEGIN
    DELETE FROM token_holders WHERE token_holders.block_hash = _hash;
    DELETE FROM wallet_events WHERE wallet_events.block_hash = _hash;
    DELETE FROM assets_summary WHERE assets_summary.block_hash = _hash;
    DELETE FROM token_transfers WHERE token_transfers.block_hash = _hash;
    DELETE FROM transactions WHERE transactions.block_hash = _hash;
    DELETE FROM logs WHERE logs.block_hash = _hash;
    DELETE FROM receipts WHERE receipts.block_hash = _hash;
    DELETE FROM accounts_state WHERE accounts_state.block_hash = _hash;
    DELETE FROM internal_transactions WHERE internal_transactions.block_hash = _hash;
    DELETE FROM uncles WHERE uncles.block_hash = _hash;
    DELETE FROM blocks WHERE blocks.hash = _hash;
    DELETE FROM chain_events WHERE block_number = number AND block_hash = _hash;
    DELETE FROM dex_logs WHERE block_number = number AND block_hash = _hash;

    RETURN TRUE;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION delete_block(number integer, _hash character varying)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
BEGIN
    DELETE FROM token_holders WHERE token_holders.block_hash = _hash;
    DELETE FROM wallet_events WHERE wallet_events.block_hash = _hash;
    DELETE FROM assets_summary WHERE assets_summary.block_hash = _hash;
    DELETE FROM token_transfers WHERE token_transfers.block_hash = _hash;
    DELETE FROM transactions WHERE transactions.block_hash = _hash;
    DELETE FROM logs WHERE logs.block_hash = _hash;
    DELETE FROM receipts WHERE receipts.block_hash = _hash;
    DELETE FROM accounts_state WHERE accounts_state.block_hash = _hash;
    DELETE FROM internal_transactions WHERE internal_transactions.block_hash = _hash;
    DELETE FROM uncles WHERE uncles.block_hash = _hash;
    DELETE FROM blocks WHERE blocks.hash = _hash;
    DELETE FROM chain_events WHERE block_number = number AND block_hash = _hash;

    RETURN TRUE;
END
$$;
-- +goose StatementEnd
