"""Add txs hashes to blocks tables

Revision ID: 9fca36eec5a5
Revises: ac27550f213f
Create Date: 2019-07-02 12:18:10.960529

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9fca36eec5a5'
down_revision = 'cdec69ddbe53'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE blocks ADD COLUMN transactions jsonb;
ALTER TABLE blocks ADD COLUMN uncles jsonb;
"""

DOWN_SQL = """
ALTER TABLE blocks DROP COLUMN uncles;
ALTER TABLE blocks DROP COLUMN transactions;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
