"""add_internal_transactions_parent_tx_hash_index

Revision ID: 89b05a04aea0
Revises: 51ea839484a7
Create Date: 2019-04-02 12:33:25.553071

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '89b05a04aea0'
down_revision = 'e3f9cd3e195e'
branch_labels = None
depends_on = None

UP_SQL = "CREATE INDEX CONCURRENTLY ix_internal_transactions_parent_tx_hash ON internal_transactions(parent_tx_hash);"
DOWN_SQL = "DROP INDEX concurrently ix_internal_transactions_parent_tx_hash;"


def upgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
