"""Remove pending txs indexes

Revision ID: 19609f5716c9
Revises: 64beb7865bf4
Create Date: 2019-07-30 11:11:16.827949

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '19609f5716c9'
down_revision = '554ff20408a1'
branch_labels = None
depends_on = None

UP_SQL = """
DROP INDEX IF EXISTS ix_pending_transactions_from_partial;
DROP INDEX IF EXISTS ix_pending_transactions_to_partial;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    pass
