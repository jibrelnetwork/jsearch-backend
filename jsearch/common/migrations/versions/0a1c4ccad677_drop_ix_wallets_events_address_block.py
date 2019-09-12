"""Drop ix_wallets_events_address_block

Revision ID: 0a1c4ccad677
Revises: d4a2a13a03ad
Create Date: 2019-09-05 14:36:01.572618

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '0a1c4ccad677'
down_revision = 'd4a2a13a03ad'
branch_labels = None
depends_on = None


UP_SQL = """
DROP INDEX CONCURRENTLY IF EXISTS ix_wallets_events_address_block;
"""

DOWN_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_wallets_events_address_block ON wallet_events(address, block_number);
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
