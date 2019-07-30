"""add logs keyset index by timestamp

Revision ID: 554ff20408a1
Revises: 94311fe6ed6b
Create Date: 2019-07-30 09:59:31.779163

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '554ff20408a1'
down_revision = '94311fe6ed6b'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_logs_keyset_by_timestamp
ON logs(address, "timestamp", transaction_index, log_index) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_logs_keyset_by_timestamp;
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
