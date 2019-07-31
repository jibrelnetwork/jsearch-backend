"""empty message

Revision ID: f9ab0ab60eb0
Revises: 562a23392b0b, 6d96e113afdc
Create Date: 2019-07-31 15:09:37.445047

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'f9ab0ab60eb0'
down_revision = ('562a23392b0b', '6d96e113afdc')
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
