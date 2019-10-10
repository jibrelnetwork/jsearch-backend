"""Add index to chain events

Revision ID: 8e7fd0b1d52c
Revises: 538fca58b088
Create Date: 2019-10-10 22:23:49.812809

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '8e7fd0b1d52c'
down_revision = '538fca58b088'
branch_labels = None
depends_on = None

UP_SQL = """
create index if not exists ix_chain_events_by_block_node_and_id on chain_events(block_number, node_id, id);
"""

DOWN_SQL = """
drop index if exists ix_chain_events_by_block_node_and_id;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
