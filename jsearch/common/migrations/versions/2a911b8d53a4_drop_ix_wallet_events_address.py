"""Drop ix_wallet_events_address

Revision ID: 2a911b8d53a4
Revises: 64e68419d0e2
Create Date: 2019-09-05 14:38:22.090298

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '2a911b8d53a4'
down_revision = '64e68419d0e2'
branch_labels = None
depends_on = None


UP_SQL = """
DROP INDEX CONCURRENTLY IF EXISTS ix_wallet_events_address;
"""

DOWN_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_address ON wallet_events(address, event_index) WHERE is_forked = false;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
