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

    RETURN TRUE;
END
$$;

CREATE OR REPLACE FUNCTION delete_blocks(range_start integer, range_end integer)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    _hash   character varying;
    _number integer;
BEGIN
    FOR _number, _hash IN
        SELECT blocks.number, blocks.hash from blocks WHERE number BETWEEN range_start and range_end
        LOOP
            PERFORM delete_block(_number, _hash);
        END LOOP;
    RETURN TRUE;
END
$$;

CREATE OR REPLACE FUNCTION remove_blocks_in_gaps(range_start integer, range_end integer)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    gap_start integer;
    gap_end   integer;
BEGIN
    FOR gap_start, gap_end IN
        SELECT gaps.gap_start, gaps.gap_end FROM get_gaps(range_start, range_end) as gaps
        LOOP
            PERFORM delete_blocks(gap_start, gap_end);
        END LOOP;
    RETURN True;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION remove_blocks_in_gaps(range_start integer, range_end integer)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    gap_start integer;
    gap_end   integer;
    _hash     character varying;
BEGIN
    FOR gap_start, gap_end IN
        SELECT gaps.gap_start, gaps.gap_end FROM get_gaps(range_start, range_end) as gaps
        LOOP
            FOR _hash IN
                SELECT chain_events.block_hash from chain_events WHERE block_number BETWEEN gap_start and gap_end
                LOOP
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
                END LOOP;
            DELETE FROM chain_events WHERE block_number BETWEEN gap_start and gap_end;
        END LOOP;
    RETURN True;
END
$$;
-- +goose StatementEnd
