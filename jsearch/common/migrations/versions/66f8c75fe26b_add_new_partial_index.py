"""Add new partial index

Revision ID: 66f8c75fe26b
Revises: 8057bd227b7c
Create Date: 2019-08-14 14:30:58.670904

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '66f8c75fe26b'
down_revision = '9bc7b4b37353'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_accounts_state_address_block_number_partial 
ON public.accounts_state USING btree (address, block_number) WHERE (is_forked = false);
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_accounts_state_address_block_number_partial;
"""


def upgrade():
    op.execute('COMMIT;')
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
