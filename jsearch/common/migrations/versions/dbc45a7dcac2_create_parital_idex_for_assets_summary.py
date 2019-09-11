"""Create parital idex for assets summary

Revision ID: dbc45a7dcac2
Revises: 2e002bdb292f
Create Date: 2019-08-21 14:49:23.916386

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'dbc45a7dcac2'
down_revision = 'c9e03f6325e7'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_asset_summary_address_asset_block_number_partial ON assets_summary USING btree (address, block_number, asset_address) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_asset_summary_address_asset_block_number_partial;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
