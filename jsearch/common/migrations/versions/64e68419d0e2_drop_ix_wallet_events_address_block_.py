"""Drop ix_wallet_events_address_block_number_event_index

Revision ID: 64e68419d0e2
Revises: 0a1c4ccad677
Create Date: 2019-09-05 14:37:22.187688

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '64e68419d0e2'
down_revision = '0a1c4ccad677'
branch_labels = None
depends_on = None


UP_SQL = """
DROP INDEX CONCURRENTLY IF EXISTS ix_wallet_events_address_block_number_event_index;
"""

DOWN_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_address_block_number_event_index ON wallet_events(address, block_number, event_index) WHERE is_forked = false;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
