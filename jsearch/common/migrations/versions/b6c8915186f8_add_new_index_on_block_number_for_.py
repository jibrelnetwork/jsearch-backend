"""Add new index on block number for wallet events

Revision ID: b6c8915186f8
Revises: c3b0dfc27a06
Create Date: 2019-06-05 22:47:57.131477

"""
from alembic import op

# revision identifiers, used by Alembic
revision = 'b6c8915186f8'
down_revision = 'c3b0dfc27a06'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallets_events_address_block ON wallet_events (address, block_number);
"""

DOWN_SQL = """
DROP INDEX ix_wallets_events_address_block;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
