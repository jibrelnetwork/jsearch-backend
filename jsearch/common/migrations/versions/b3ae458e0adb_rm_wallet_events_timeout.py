"""rm_wallet_events_timeout

Revision ID: b3ae458e0adb
Revises: b1863b60ea3c
Create Date: 2019-08-16 14:33:07.198709

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'b3ae458e0adb'
down_revision = 'b1863b60ea3c'
branch_labels = None
depends_on = None


UP_SQL = """
DROP INDEX IF EXISTS ix_wallet_events_address_timestamp_event_index;
ALTER TABLE wallet_events DROP COLUMN "timestamp";
"""

DOWN_SQL = """
ALTER TABLE wallet_events ADD COLUMN "timestamp" integer;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
