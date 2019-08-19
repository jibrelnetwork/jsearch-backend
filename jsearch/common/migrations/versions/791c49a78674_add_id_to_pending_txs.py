"""Add id to pending txs

Revision ID: 791c49a78674
Revises: 19609f5716c9
Create Date: 2019-07-30 11:14:13.136668

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '791c49a78674'
down_revision = '19609f5716c9'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE pending_transactions ADD COLUMN id SERIAL;
"""

DOWN_SQL = """
ALTER TABLE pending_transactions DROP COLUMN id;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
