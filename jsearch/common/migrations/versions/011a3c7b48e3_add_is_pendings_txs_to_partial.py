"""add is pendings txs to partial

Revision ID: 011a3c7b48e3
Revises: 54d6212b1db9
Create Date: 2019-04-24 13:45:02.895357

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '011a3c7b48e3'
down_revision = '54d6212b1db9'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pending_transactions_to_partial ON pending_transactions("to") WHERE "removed" IS FALSE;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY IF EXISTS ix_pending_transactions_to_partial; 
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
