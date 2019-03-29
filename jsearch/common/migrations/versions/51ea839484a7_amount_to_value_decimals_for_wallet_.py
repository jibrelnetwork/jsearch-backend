"""amount to value-decimals for wallet tables

Revision ID: 51ea839484a7
Revises: af0dbdab309a
Create Date: 2019-03-28 17:03:06.617746

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '51ea839484a7'
down_revision = 'af0dbdab309a'
branch_labels = None
depends_on = None


UP_SQL = """

ALTER TABLE assets_transfers ADD COLUMN value NUMERIC DEFAULT null;
ALTER TABLE assets_transfers ADD COLUMN decimals integer DEFAULT null;
ALTER TABLE assets_transfers DROP COLUMN amount;

ALTER TABLE assets_summary ADD COLUMN value NUMERIC DEFAULT null;
ALTER TABLE assets_summary ADD COLUMN decimals integer DEFAULT null;
ALTER TABLE assets_summary DROP COLUMN balance;
"""


DOWN_SQL = """
ALTER TABLE assets_transfers DROP COLUMN value;
ALTER TABLE assets_transfers DROP COLUMN decimals;
ALTER TABLE assets_transfers ADD COLUMN amount varchar DEFAULT null;

ALTER TABLE assets_summary DROP COLUMN value;
ALTER TABLE assets_summary DROP COLUMN decimals;
ALTER TABLE assets_summary ADD COLUMN balance varchar DEFAULT null;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)

