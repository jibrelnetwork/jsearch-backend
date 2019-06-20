"""empty message

Revision ID: 44cbe712c438
Revises: 1e904d9a7b04, 165502227468
Create Date: 2019-05-20 10:06:05.824479

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '44cbe712c438'
down_revision = ('1e904d9a7b04', '165502227468')
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
