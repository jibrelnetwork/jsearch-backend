"""add id to token holders

Revision ID: 21c497d5d0cb
Revises: 0d12cb1ee33d
Create Date: 2019-08-06 12:52:15.025861

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '21c497d5d0cb'
down_revision = '0d12cb1ee33d'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE token_holders ADD COLUMN "id" SERIAL;
"""

DOWN_SQL = """
ALTER TABLE token_holders DROP COLUMN "id";
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
