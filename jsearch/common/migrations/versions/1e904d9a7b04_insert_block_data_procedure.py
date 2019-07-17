"""insert_block_data_procedure

Revision ID: 1e904d9a7b04
Revises: 86dd387f8474
Create Date: 2019-05-16 08:45:23.137394

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '1e904d9a7b04'
down_revision = '86dd387f8474'
branch_labels = None
depends_on = None


UP_SQL = """
alter table token_transfers drop constraint token_transfers_unique;

CREATE OR REPLACE FUNCTION insert_block_data(
			block_data text,
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
            assets_summary_updates_data text
            )
RETURNS BOOLEAN 
AS $$
BEGIN
	INSERT INTO blocks SELECT * FROM json_populate_recordset(null::blocks, block_data::json);

	IF json_array_length(uncles_data::json) > 0 THEN
		INSERT INTO uncles SELECT * FROM json_populate_recordset(null::uncles, uncles_data::json);
	END IF;

	IF json_array_length(transactions_data::json) > 0 THEN
		INSERT INTO transactions SELECT * FROM json_populate_recordset(null::transactions, transactions_data::json);
	END IF;

	IF json_array_length(receipts_data::json) > 0 THEN
		INSERT INTO receipts SELECT * FROM json_populate_recordset(null::receipts, receipts_data::json);
	END IF;

	IF json_array_length(logs_data::json) > 0 THEN
		INSERT INTO logs SELECT * FROM json_populate_recordset(null::logs, logs_data::json);
	END IF;

	IF json_array_length(accounts_state_data::json) > 0 THEN
		INSERT INTO accounts_state SELECT * FROM json_populate_recordset(null::accounts_state, accounts_state_data::json);
	END IF;

	IF json_array_length(accounts_base_data::json) > 0 THEN
		INSERT INTO accounts_base SELECT * FROM json_populate_recordset(null::accounts_base, accounts_base_data::json);
	END IF;

	IF json_array_length(internal_txs_data::json) > 0 THEN
		INSERT INTO internal_transactions SELECT * FROM json_populate_recordset(null::internal_transactions, internal_txs_data::json);
	END IF;

	IF json_array_length(transfers_data::json) > 0 THEN
		INSERT INTO token_transfers SELECT * FROM json_populate_recordset(null::token_transfers, transfers_data::json);
	END IF;

	IF json_array_length(token_holders_updates_data::json) > 0 THEN
		INSERT INTO token_holders SELECT * FROM json_populate_recordset(null::token_holders, token_holders_updates_data::json)
			ON CONFLICT (account_address, token_address) DO UPDATE SET balance = EXCLUDED.balance;
	END IF;
	
	IF json_array_length(wallet_events_data::json) > 0 THEN
		INSERT INTO wallet_events SELECT * FROM json_populate_recordset(null::wallet_events, wallet_events_data::json);
	END IF;
	
	IF json_array_length(assets_summary_updates_data::json) > 0 THEN
		INSERT INTO assets_summary SELECT * FROM json_populate_recordset(null::assets_summary, assets_summary_updates_data::json)
		    ON CONFLICT (address, asset_address) DO UPDATE SET value = EXCLUDED.value;
	END IF;
	RETURN true;
END;
$$
LANGUAGE plpgsql;
"""

DOWN_SQL = """
alter table token_transfers add constraint token_transfers_unique  unique (transaction_hash, log_index, address);

DROP FUNCTION IF EXISTS insert_block_data;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
