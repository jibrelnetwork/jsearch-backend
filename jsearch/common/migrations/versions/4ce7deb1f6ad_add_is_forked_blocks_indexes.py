"""Add is_forked blocks indexes

Revision ID: 4ce7deb1f6ad
Revises: 3fc09e182d00
Create Date: 2019-10-07 12:49:19.935519

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '4ce7deb1f6ad'
down_revision = '3fc09e182d00'
branch_labels = None
depends_on = None

UP_SQL = """
create index concurrently if not exists ix_blocks_number on blocks(number) where is_forked = false;
"""

DOWN_SQL = """
select 1;
"""


def upgrade():
    op.execute('commit;')
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
