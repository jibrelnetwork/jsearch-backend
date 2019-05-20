"""Add field status to transactions

Revision ID: c3b0dfc27a06
Revises: 165502227468
Create Date: 2019-05-17 15:45:46.638092

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'c3b0dfc27a06'
down_revision = '165502227468'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE transactions ADD COLUMN status integer;
"""

DOWN_SQL = """
ALTER TABLE transactions DROP COLUMN status;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
