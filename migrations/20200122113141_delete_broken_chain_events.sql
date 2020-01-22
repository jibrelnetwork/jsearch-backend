-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

CREATE OR REPLACE FUNCTION get_gaps(range_start integer, range_end integer)
    RETURNS TABLE
            (
                gap_start   integer,
                gap_end     integer,
                parent_hash character varying,
                prev_hash   character varying
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY SELECT numbers.number - 1,
                        numbers.next_number + 1,
                        numbers.parent_hash,
                        numbers.prev_hash
                 FROM (
                          SELECT blocks.number,
                                 blocks.parent_hash,
                                 lead(number) OVER (ORDER BY number DESC) as next_number,
                                 lead(hash) OVER (ORDER BY number DESC)   as prev_hash
                          FROM blocks
                          WHERE is_forked = FALSE
                            and blocks.number >= range_start
                            and blocks.number <= range_end
                          ORDER BY number DESC
                      ) numbers
                 WHERE numbers.parent_hash <> numbers.prev_hash;
END
$$;

CREATE OR REPLACE FUNCTION remove_blocks_in_gaps(range_start integer, range_end integer)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    gap_start integer;
    gap_end integer;
    _hash character varying;
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

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION get_gaps;
DROP FUNCTION remove_blocks_in_gaps;
-- +goose StatementEnd
