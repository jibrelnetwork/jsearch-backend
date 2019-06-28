"""drop_internal_txs_pk_without_call_depth

Revision ID: 389563d0baee
Revises: e14bb549ec28
Create Date: 2019-06-28 09:58:02.367000

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '389563d0baee'
down_revision = 'e14bb549ec28'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE internal_transactions DROP CONSTRAINT internal_transactions_pkey;
"""

DOWN_SQL = """
ALTER TABLE internal_transactions ADD PRIMARY KEY (block_hash, parent_tx_hash, transaction_index);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
