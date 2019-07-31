"""Add pending txs index from

Revision ID: 0e256b6ceec4
Revises: 72071bdc6c60
Create Date: 2019-07-30 11:18:06.055481

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '0e256b6ceec4'
down_revision = '72071bdc6c60'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pending_txs_from_partial ON pending_transactions("from", "timestamp", "id") WHERE "removed" = FALSE;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_pending_txs_from_partial;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
