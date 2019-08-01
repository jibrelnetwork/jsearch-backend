"""add ix_blocks_timestamp

Revision ID: 0d12cb1ee33d
Revises: 562a23392b0b
Create Date: 2019-08-01 13:28:04.839750

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '0d12cb1ee33d'
down_revision = '562a23392b0b'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_blocks_timestamp ON blocks("timestamp");
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_blocks_timestamp;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
