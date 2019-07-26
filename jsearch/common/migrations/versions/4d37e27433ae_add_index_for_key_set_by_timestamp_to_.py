"""Add index for key set by timestamp to internal_txs

Revision ID: 4d37e27433ae
Revises: 5587ad09c507
Create Date: 2019-07-25 14:54:12.921256

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '4d37e27433ae'
down_revision = '5587ad09c507'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_transactions_keyset_by_timestamp
ON internal_transactions(tx_origin, "timestamp", parent_tx_index, transaction_index) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_transactions_keyset_by_timestamp;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
