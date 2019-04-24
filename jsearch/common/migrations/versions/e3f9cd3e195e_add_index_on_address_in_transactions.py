"""empty message

Revision ID: e3f9cd3e195e
Revises: 51ea839484a7
Create Date: 2019-04-01 15:27:51.432348

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = 'e3f9cd3e195e'
down_revision = '51ea839484a7'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS transactions_address_idx_partial ON public.transactions
    USING btree (address, block_number, transaction_index) where is_forked = false;
"""

DOWN_SQL = """DROP INDEX CONCURRENTLY IF EXISTS transactions_address_idx_partial;"""


def upgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
