"""empty message

Revision ID: 90822e2eee41
Revises: c3b0dfc27a06, 44cbe712c438
Create Date: 2019-05-21 12:36:22.849307

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '90822e2eee41'
down_revision = ('c3b0dfc27a06', '44cbe712c438')
branch_labels = None
depends_on = None


UP_SQL = """

"""

DOWN_SQL = """

"""


def upgrade():
    pass


def downgrade():
    pass
