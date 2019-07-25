"""Add index for key set by block to internal_txs

Revision ID: 64beb7865bf4
Revises: 4d37e27433ae
Create Date: 2019-07-25 14:55:32.105563

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '64beb7865bf4'
down_revision = '4d37e27433ae'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX ix_transactions_keyset_by_block
ON internal_transactions(tx_origin, block_number, parent_tx_index, transaction_index) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX ix_transactions_keyset_by_block;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
