"""Add parent_tx_index to internal_txs

Revision ID: 5587ad09c507
Revises: f50be7d3334d
Create Date: 2019-07-25 14:44:55.700960

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '5587ad09c507'
down_revision = 'f50be7d3334d'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE internal_transactions ADD COLUMN parent_tx_index integer;

UPDATE internal_transactions SET parent_tx_index = txs.transaction_index
FROM transactions as txs 
WHERE txs.hash = internal_transactions.parent_tx_hash;
"""

DOWN_SQL = """
ALTER TABLE internal_transactions DROP COLUMN parent_tx_index;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
