"""add pending events

Revision ID: 6a8903983350
Revises: 165502227468
Create Date: 2019-05-14 19:18:44.209353

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '6a8903983350'
down_revision = '165502227468'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE IF NOT EXISTS pending_wallet_events(
    is_removed boolean default false,
    address varchar,
    type varchar,
    tx_hash varchar,
    block_hash varchar,
    block_number bigint,
    event_index bigint,
    tx_data json,
    event_data json
);

"""

DOWN_SQL = """
DROP TABLE pending_wallet_events;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
