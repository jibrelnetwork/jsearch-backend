"""fix assets value field type

Revision ID: af0dbdab309a
Revises: cea942470181
Create Date: 2019-03-18 13:05:53.815428

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'af0dbdab309a'
down_revision = '9c64aeb20d0e'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE assets_transfers ALTER COLUMN amount SET DATA TYPE varchar;
ALTER TABLE assets_summary ALTER COLUMN balance SET DATA TYPE varchar;

ALTER TABLE assets_summary ADD PRIMARY KEY (address, asset_address);
"""

DOWN_SQL = """
ALTER TABLE assets_transfers DROP COLUMN amount;
ALTER TABLE assets_summary DROP COLUMN balance;
ALTER TABLE assets_transfers ADD COLUMN amount NUMERIC;
ALTER TABLE assets_summary ADD COLUMN balance NUMERIC;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
