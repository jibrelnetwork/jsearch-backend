"""add status for token transfers

Revision ID: 4c2d1b91374f
Revises: 3fa2fddad011
Create Date: 2019-04-16 07:14:24.968029

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '4c2d1b91374f'
down_revision = '3fa2fddad011'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE token_transfers ADD COLUMN status integer;
"""


DOWN_SQL = """
ALTER TABLE token_transfers DROP COLUMN status;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
