"""add missing timestamps

Revision ID: 90d4195a9021
Revises: 554ff20408a1
Create Date: 2019-07-30 10:17:00.187505

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '90d4195a9021'
down_revision = '7ad9df46ab41'
branch_labels = None
depends_on = None


UP_SQL = """
UPDATE logs SET "timestamp" = blocks.timestamp 
FROM blocks 
WHERE logs.block_hash = blocks.hash;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    pass
