"""

Revision ID: fbfdcf501ab4
Revises: f9ab0ab60eb0
Create Date: 2019-08-01 13:59:45.299363

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'fbfdcf501ab4'
down_revision = 'f9ab0ab60eb0'
branch_labels = None
depends_on = None


UP_SQL ="""
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallet_events_address_timestamp_event_index ON wallet_events(address, "timestamp", event_index)
    WHERE is_forked=false;
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_wallet_events_address_timestamp_event_index;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
