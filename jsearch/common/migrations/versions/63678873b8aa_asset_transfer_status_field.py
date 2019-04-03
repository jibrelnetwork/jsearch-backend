"""asset_transfer status field

Revision ID: 63678873b8aa
Revises: 51ea839484a7
Create Date: 2019-04-02 18:24:11.423492

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '63678873b8aa'
down_revision = '51ea839484a7'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE assets_transfers ADD COLUMN status integer;
"""


DOWN_SQL = """
ALTER TABLE assets_transfers DROP COLUMN status;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
