"""reorg.node_id

Revision ID: 8895b229311d
Revises: 9f4d8f8f7f24
Create Date: 2018-12-26 19:58:17.156663

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '8895b229311d'
down_revision = '9f4d8f8f7f24'
branch_labels = None
depends_on = None


UP_SQL = """ALTER TABLE reorgs ADD COLUMN node_id varchar(70) NOT NULL"""
DOWN_SQL = """ALTER TABLE reorgs DROP COLUMN node_id"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
   op.execute(DOWN_SQL)