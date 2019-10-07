"""Change blocks indexes

Revision ID: 3fc09e182d00
Revises: 6cbbf0051aa0
Create Date: 2019-10-07 12:45:31.791836

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '3fc09e182d00'
down_revision = '6cbbf0051aa0'
branch_labels = None
depends_on = None


UP_SQL = """
drop index if exists ix_blocks_is_forked;
drop index if exists ix_blocks_number; 
drop index if exists ix_blocks_timestamp; 
"""

DOWN_SQL = """
select 1;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
