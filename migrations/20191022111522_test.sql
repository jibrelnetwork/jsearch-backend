-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION test_time() RETURNS varchar
    LANGUAGE plpgsql
AS
$$
DECLARE
    start_time timestamp = clock_timestamp();
BEGIN
    RAISE LOG 'time %s', clock_timestamp() - start_time;
    RETURN clock_timestamp() - start_time;
END;
$$;

CREATE OR REPLACE FUNCTION insert_block_data(block_data text,
                                             uncles_data text,
                                             transactions_data text,
                                             receipts_data text,
                                             logs_data text,
                                             accounts_state_data text,
                                             accounts_base_data text,
                                             internal_txs_data text,
                                             transfers_data text,
                                             token_holders_updates_data text,
                                             wallet_events_data text,
                                             assets_summary_updates_data text,
                                             chain_event_data text) RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    start_time timestamp = clock_timestamp();
BEGIN
    INSERT INTO blocks SELECT * FROM json_populate_recordset(null::blocks, block_data::json);
    RAISE LOG 'INSERT blocks %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(uncles_data::json) > 0 THEN
        INSERT INTO uncles SELECT * FROM json_populate_recordset(null::uncles, uncles_data::json);
    END IF;
    RAISE LOG 'INSERT uncles %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(transactions_data::json) > 0 THEN
        INSERT INTO transactions SELECT * FROM json_populate_recordset(null::transactions, transactions_data::json);
    END IF;
    RAISE LOG 'INSERT transactions %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(receipts_data::json) > 0 THEN
        INSERT INTO receipts SELECT * FROM json_populate_recordset(null::receipts, receipts_data::json);
    END IF;
    RAISE LOG 'INSERT receipts %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(logs_data::json) > 0 THEN
        INSERT INTO logs SELECT * FROM json_populate_recordset(null::logs, logs_data::json);
    END IF;
    RAISE LOG 'INSERT logs %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(accounts_state_data::json) > 0 THEN
        INSERT INTO accounts_state
        SELECT *
        FROM json_populate_recordset(null::accounts_state, accounts_state_data::json);
    END IF;
    RAISE LOG 'INSERT accounts_state %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(accounts_base_data::json) > 0 THEN
        INSERT INTO accounts_base SELECT * FROM json_populate_recordset(null::accounts_base, accounts_base_data::json);
    END IF;
    RAISE LOG 'INSERT accounts_base %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(internal_txs_data::json) > 0 THEN
        INSERT INTO internal_transactions
        SELECT *
        FROM json_populate_recordset(null::internal_transactions, internal_txs_data::json);
    END IF;
    RAISE LOG 'INSERT internal_transactions %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(transfers_data::json) > 0 THEN
        INSERT INTO token_transfers SELECT * FROM json_populate_recordset(null::token_transfers, transfers_data::json);
    END IF;
    RAISE LOG 'INSERT token_transfers %s', clock_timestamp() - start_time;

    /*
        `SELECT account_address, token_address, balance, decimals, block_number` -
        need to prevent insert null to `id` field which will cause error.
    */
    start_time = clock_timestamp();
    IF json_array_length(token_holders_updates_data::json) > 0 THEN
        INSERT INTO token_holders (account_address, token_address, balance, decimals, block_number, block_hash,
                                   is_forked)
        SELECT account_address,
               token_address,
               balance,
               decimals,
               block_number,
               block_hash,
               CASE WHEN is_forked is null THEN false ELSE is_forked END as is_forked
        FROM json_populate_recordset(null::token_holders, token_holders_updates_data::json)
        ON CONFLICT (account_address, token_address, block_hash)
            DO UPDATE SET balance = EXCLUDED.balance, block_number = EXCLUDED.block_number
        WHERE token_holders.block_number is null
           or token_holders.block_number < EXCLUDED.block_number;
    END IF;
    RAISE LOG 'INSERT token_holders %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(wallet_events_data::json) > 0 THEN
        INSERT INTO wallet_events SELECT * FROM json_populate_recordset(null::wallet_events, wallet_events_data::json);
    END IF;
    RAISE LOG 'INSERT wallet_events %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF json_array_length(assets_summary_updates_data::json) > 0 THEN
        INSERT INTO assets_summary (address, asset_address, "value", decimals, nonce, tx_number, block_number,
                                    block_hash, is_forked)
        SELECT address,
               asset_address,
               "value",
               decimals,
               nonce,
               tx_number,
               block_number,
               block_hash,
               CASE WHEN is_forked is null THEN false ELSE is_forked END as is_forked
        FROM json_populate_recordset(null::assets_summary, assets_summary_updates_data::json)
        ON CONFLICT (address, asset_address, block_hash)
            DO UPDATE SET value        = EXCLUDED.value,
                          block_number = EXCLUDED.block_number,
                          nonce        = EXCLUDED.nonce,
                          tx_number    = EXCLUDED.tx_number
        WHERE assets_summary.block_number is null
           or assets_summary.block_number < EXCLUDED.block_number;
    END IF;
    RAISE LOG 'INSERT assets_summary %s', clock_timestamp() - start_time;

    start_time = clock_timestamp();
    IF char_length(chain_event_data) > 2 THEN
        INSERT INTO chain_events SELECT * FROM json_populate_record(null::chain_events, chain_event_data::json);
    END IF;
    RAISE LOG 'INSERT chain_event %s', clock_timestamp() - start_time;

    RETURN true;
END;
$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION test_time;
-- +goose StatementEnd
