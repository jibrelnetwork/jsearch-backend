"""

Revision ID: 6d96e113afdc
Revises: 64beb7865bf4
Create Date: 2019-07-30 15:38:15.199212

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6d96e113afdc'
down_revision = '64beb7865bf4'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE wallet_events ADD COLUMN "timestamp" integer;
"""

DOWN_SQL = """
ALTER TABLE wallet_events DROP COLUMN "timestamp";
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)

