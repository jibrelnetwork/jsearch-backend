"""empty message

Revision ID: f9ab0ab60eb0
Revises: 562a23392b0b, 6d96e113afdc
Create Date: 2019-07-31 15:09:37.445047

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'f9ab0ab60eb0'
down_revision = ('562a23392b0b', '6d96e113afdc')
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_address_block_number_event_index ON wallet_events(address, block_number, event_index)
    WHERE is_forked=false;
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_wallet_events_address_block_number_event_index;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
