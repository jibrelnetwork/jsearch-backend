"""Add is_forked blocks by timestamp indexes

Revision ID: 538fca58b088
Revises: 4ce7deb1f6ad
Create Date: 2019-10-07 12:50:14.518675

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '538fca58b088'
down_revision = '4ce7deb1f6ad'
branch_labels = None
depends_on = None

UP_SQL = """
create index concurrently if not exists ix_blocks_timestamp on blocks(number) where is_forked = false;
"""

DOWN_SQL = """
select 1;
"""


def upgrade():
    op.execute('commit;')
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
