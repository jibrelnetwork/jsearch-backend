"""add is pendings txs from partial

Revision ID: 54d6212b1db9
Revises: 7f7ef6abbe8f
Create Date: 2019-04-24 13:43:52.175961

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '54d6212b1db9'
down_revision = '7f7ef6abbe8f'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_pending_transactions_from_partial ON pending_transactions("from") WHERE "removed" IS FALSE;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_pending_transactions_from_partial;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
