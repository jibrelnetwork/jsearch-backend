"""add_internal_txs_pk_with_call_depth

Revision ID: 796abd73d069
Revises: 995ab1bc1218
Create Date: 2019-06-28 10:49:19.991463

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '796abd73d069'
down_revision = '995ab1bc1218'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE internal_transactions ADD PRIMARY KEY USING INDEX internal_transactions_pkey;
"""

DOWN_SQL = """
ALTER TABLE internal_transactions DROP CONSTRAINT internal_transactions_pkey;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
