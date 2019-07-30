"""add timestamp

Revision ID: 7ad9df46ab41
Revises: 64beb7865bf4
Create Date: 2019-07-30 09:56:36.588459

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '7ad9df46ab41'
down_revision = '64beb7865bf4'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE logs ADD COLUMN "timestamp" integer;
"""

DOWN_SQL = """
ALTER TABLE logs DROP COLUMN "timestamp";
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
