"""empty message

Revision ID: 955fc8771527
Revises: d4a2a13a03ad
Create Date: 2019-09-12 10:28:16.499121

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '955fc8771527'
down_revision = 'a91c553a7fbc'
branch_labels = None
depends_on = None

UP_SQL = """
select pg_sleep(10800);
"""

DOWN_SQL = """
select 1;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
