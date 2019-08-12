"""add new index by token holders

Revision ID: 1f1768ae5bd5
Revises: 21c497d5d0cb
Create Date: 2019-08-06 12:53:25.888177

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '1f1768ae5bd5'
down_revision = '21c497d5d0cb'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_token_holders_token_balance_id on token_holders(token_address, balance, "id");
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_token_holders_token_balance_id;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
