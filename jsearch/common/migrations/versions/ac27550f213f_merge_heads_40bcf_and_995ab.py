"""merge heads 40bcf and 995ab

Revision ID: ac27550f213f
Revises: 40bcf4f66ab9, 995ab1bc1218
Create Date: 2019-07-01 10:05:33.082024

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'ac27550f213f'
down_revision = ('40bcf4f66ab9', '995ab1bc1218')
branch_labels = None
depends_on = None


UP_SQL = """

"""

DOWN_SQL = """

"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
