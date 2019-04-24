"""add assets pending txs last synced

Revision ID: 7f7ef6abbe8f
Revises: 0bbb93975419
Create Date: 2019-04-24 13:42:08.753303

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '7f7ef6abbe8f'
down_revision = 'eb4c04c8de9f'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pending_transactions_last_synced_id ON pending_transactions("last_synced_id");
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_pending_transactions_last_synced_id;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
