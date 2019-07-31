"""Add pending txs index from

Revision ID: 72071bdc6c60
Revises: 791c49a78674
Create Date: 2019-07-30 11:15:35.190378

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '72071bdc6c60'
down_revision = '791c49a78674'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pending_txs_to_partial ON pending_transactions("to", "timestamp", "id") WHERE "removed" = FALSE;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_pending_txs_to_partial; 
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
