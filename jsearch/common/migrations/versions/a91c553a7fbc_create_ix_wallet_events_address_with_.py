"""Create ix_wallet_events_address with type

Revision ID: a91c553a7fbc
Revises: 2a911b8d53a4
Create Date: 2019-09-05 14:40:43.375310

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'a91c553a7fbc'
down_revision = '2a911b8d53a4'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_address_event_index_type_partial ON wallet_events(address, event_index, type) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY IF EXISTS ix_wallet_events_address_event_index_type_partial;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
