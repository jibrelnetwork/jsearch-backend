"""Add partial index for token holders

Revision ID: 1c8fb14439fd
Revises: c9e03f6325e7
Create Date: 2019-08-23 12:44:19.227593

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '1c8fb14439fd'
down_revision = 'dbc45a7dcac2'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_token_holders_by_token_block_number_and_address 
ON token_holders USING btree (token_address, block_number, account_address) WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX IF EXISTS ix_token_holders_by_token_block_number_and_address; 
"""


def upgrade():
    op.execute("COMMIT;")
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
