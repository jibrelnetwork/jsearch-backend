"""token_transfers_transaction_index_column

Revision ID: 4377c859182d
Revises: d44ee9b43f8d
Create Date: 2019-01-17 12:43:40.333265

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '4377c859182d'
down_revision = 'd44ee9b43f8d'
branch_labels = None
depends_on = None

UP_SQL = """

            DROP INDEX ix_token_transfers_address_block_number_log_index;
            DROP INDEX ix_token_transfers_token_address_block_number_log_index;
            
            ALTER TABLE token_transfers ADD COLUMN transaction_index integer;
            
            CREATE INDEX ix_token_transfers_address_block_number_log_index ON token_transfers(address, block_number, transaction_index, log_index);
            CREATE INDEX ix_token_transfers_token_address_block_number_log_index ON token_transfers(token_address, block_number, transaction_index, log_index);
            """
DOWN_SQL = """
            DROP INDEX ix_token_transfers_address_block_number_log_index;
            DROP INDEX ix_token_transfers_block_hash;
            """


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)