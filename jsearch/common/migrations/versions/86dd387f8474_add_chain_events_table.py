"""add_chain_events_table

Revision ID: 86dd387f8474
Revises: 0bbb93975419
Create Date: 2019-04-26 07:50:28.723799

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '86dd387f8474'
down_revision = '0bbb93975419'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE TABLE chain_events (
    id bigint PRIMARY KEY,
    block_hash varchar,
    block_number bigint,
    event_type varchar,
    reorg_id bigint,
    split_id bigint,
    node_id varchar,
    created_at timestamp
);
"""

DOWN_SQL = """
DROP TABLE chain_events;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
