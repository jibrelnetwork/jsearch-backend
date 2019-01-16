"""empty message

Revision ID: 2b3b48c8762b
Revises: 663cb3c4d3ba
Create Date: 2019-01-11 18:41:15.904692

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '2b3b48c8762b'
down_revision = '663cb3c4d3ba'
branch_labels = None
depends_on = None

UP_SQL = """ALTER TABLE token_holders ADD COLUMN decimals integer;"""
DOWN_SQL = """ALTER TABLE token_holders DROP COLUMN decimals"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
