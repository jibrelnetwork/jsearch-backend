"""add wallet events index by address event_index

Revision ID: b1c7a792d0e5
Revises: 0bbb93975419
Create Date: 2019-04-24 13:51:46.811402

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b1c7a792d0e5'
down_revision = '0bbb93975419'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_block_hash ON wallet_events (block_hash);
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_wallet_events_block_hash;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
