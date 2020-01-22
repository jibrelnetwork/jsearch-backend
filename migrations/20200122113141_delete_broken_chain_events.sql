-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

CREATE FUNCTION get_gaps(range_start integer, range_end integer)
    RETURNS TABLE
            (
                gap_start   bigint,
                gap_end     bigint,
                parent_hash character varying,
                prev_hash   character varying
            )
    LANGUAGE plpgsql
AS
$$
BEGIN
    RETURN QUERY SELECT number - 1,
                        next_number + 1,
                        parent_hash,
                        prev_hash
                 FROM (
                          SELECT number,
                                 parent_hash,
                                 lead(number) OVER (ORDER BY number DESC) as next_number,
                                 lead(hash) OVER (ORDER BY number DESC)   as prev_hash
                          FROM blocks
                          WHERE is_forked = FALSE
                            and blocks.number >= range_start
                            and blocks.number <= range_end
                          ORDER BY number DESC
                      ) numbers
                 WHERE parent_hash <> prev_hash;
END
$$;

CREATE FUNCTION remove_blocks_in_gaps(range_star integer, range_end integer)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    gap_start integer;
    gap_end integer;
    block_hash character varying;
BEGIN
    FOR gap_start, gap_end IN
        SELECT gap_start, gap_end FROM get_gaps(range_start, range_end)
        LOOP
            FOR block_hash IN SELECT block_hash from chain_events WHERE block_number BETWEEN gap_start and gap_end
                LOOP
                    DELETE FROM token_holders WHERE block_hash = block_hash;
                    DELETE FROM wallet_events WHERE block_hash = block_hash;
                    DELETE FROM assets_summary WHERE block_hash = block_hash;
                    DELETE FROM token_transfers WHERE block_hash = block_hash;
                    DELETE FROM transactions WHERE block_hash = block_hash;
                    DELETE FROM logs WHERE block_hash = block_hash;
                    DELETE FROM receipts WHERE block_hash = block_hash;
                    DELETE FROM accounts_state WHERE block_hash = block_hash;
                    DELETE FROM internal_transactions WHERE block_hash = block_hash;
                    DELETE FROM uncles WHERE block_hash = block_hash;
                    DELETE FROM blocks WHERE block_hash = block_hash;
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
