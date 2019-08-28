"""Drop unique constraint for token holders

Revision ID: 2e002bdb292f
Revises: b1863b60ea3c
Create Date: 2019-08-21 14:01:12.272674

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '2e002bdb292f'
down_revision = '6a924e0a5f2b'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE assets_summary ADD COLUMN is_forked BOOLEAN;
ALTER TABLE assets_summary ALTER COLUMN is_forked SET DEFAULT false;
ALTER TABLE assets_summary ADD COLUMN block_hash VARCHAR;
ALTER TABLE assets_summary DROP CONSTRAINT assets_summary_pkey;
CREATE UNIQUE INDEX assets_summary_by_block_hash on assets_summary(block_hash, asset_address, address);
"""

DOWN_SQL = """
ALTER TABLE assets_summary DROP CONSTRAINT if exists assets_summary_by_block_hash;
ALTER TABLE assets_summary DROP COLUMN is_forked;
ALTER TABLE assets_summary DROP COLUMN block_hash;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
