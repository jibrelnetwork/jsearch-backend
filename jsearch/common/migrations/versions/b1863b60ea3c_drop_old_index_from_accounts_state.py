"""Drop old index from accounts state

Revision ID: b1863b60ea3c
Revises: 66f8c75fe26b
Create Date: 2019-08-14 14:34:51.265115

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b1863b60ea3c'
down_revision = '66f8c75fe26b'
branch_labels = None
depends_on = None

UP_SQL = """
DROP INDEX IF EXISTS ix_accounts_state_address_block_number;
DROP INDEX IF EXISTS ix_accounts_state_is_forked;
"""

DOWN_SQL = """
CREATE INDEX IF NOT EXISTS ix_accounts_state_address_block_number 
ON accounts_state USING btree (address, block_number);

CREATE INDEX ix_accounts_state_is_forked ON accounts_state USING btree (is_forked);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
