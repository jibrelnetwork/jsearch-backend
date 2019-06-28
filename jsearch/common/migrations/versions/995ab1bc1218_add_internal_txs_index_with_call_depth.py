"""add_internal_txs_index_with_call_depth

Revision ID: 995ab1bc1218
Revises: 389563d0baee
Create Date: 2019-06-28 10:42:27.712813

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '995ab1bc1218'
down_revision = '389563d0baee'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE UNIQUE INDEX CONCURRENTLY internal_transactions_pkey
  ON internal_transactions(block_hash, parent_tx_hash, transaction_index, call_depth);
"""

DOWN_SQL = """
CREATE UNIQUE INDEX CONCURRENTLY internal_transactions_pkey
  ON internal_transactions(block_hash, parent_tx_hash, transaction_index);
"""


def upgrade():
    op.execute('COMMIT')
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')
    op.execute(DOWN_SQL)
