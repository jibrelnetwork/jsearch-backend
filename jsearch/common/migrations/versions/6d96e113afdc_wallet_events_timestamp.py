"""

Revision ID: 6d96e113afdc
Revises: 64beb7865bf4
Create Date: 2019-07-30 15:38:15.199212

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6d96e113afdc'
down_revision = '64beb7865bf4'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE wallet_events ADD COLUMN "timestamp" integer;

CREATE INDEX ix_wallet_events_address_timestamp_event_index ON wallet_events(address, "timestamp", event_index)
    WHERE is_forked=false;
CREATE INDEX ix_wallet_events_address_block_number_event_index ON wallet_events(address, block_number, event_index)
    WHERE is_forked=false;
    
DROP INDEX IF EXISTS ix_wallets_events_address_block;
"""

DOWN_SQL = """
ALTER TABLE wallet_events DROP COLUMN "timestamp";

DROP INDEX IF EXISTS ix_wallet_events_address_timestamp_event_index;
DROP INDEX IF EXISTS ix_wallet_events_address_block_number_event_index;

CREATE INDEX ix_wallets_events_address_block ON  wallet_events(address, block_number);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
