"""add logs keyset index by block

Revision ID: 94311fe6ed6b
Revises: 7ad9df46ab41
Create Date: 2019-07-30 09:59:26.749378

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '94311fe6ed6b'
down_revision = '90d4195a9021'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY ix_logs_keyset_by_block
ON logs(address, block_number, transaction_index, log_index) WHERE is_forked = false
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_logs_keyset_by_block
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
