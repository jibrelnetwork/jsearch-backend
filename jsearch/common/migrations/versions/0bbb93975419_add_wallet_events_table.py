"""add_wallet_events_table

Revision ID: 0bbb93975419
Revises: 63678873b8aa
Create Date: 2019-04-10 13:56:06.305934

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '0bbb93975419'
down_revision = '4c2d1b91374f'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE IF NOT EXISTS wallet_events(
    address varchar,
    type varchar,
    tx_hash varchar,
    block_hash varchar,
    block_number bigint,
    event_index bigint,
    is_forked boolean default false,
    tx_data json,
    event_data json
);

"""

DOWN_SQL = """
DROP TABLE wallet_events;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
