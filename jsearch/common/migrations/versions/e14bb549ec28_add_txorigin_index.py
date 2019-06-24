"""empty message

Revision ID: e14bb549ec28
Revises: dd24e9cfed68
Create Date: 2019-06-21 15:25:33.656704

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'e14bb549ec28'
down_revision = 'dd24e9cfed68'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE INDEX CONCURRENTLY ix_internal_transactions_tx_origin
ON internal_transactions("tx_origin")
WHERE is_forked = false;
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_internal_transactions_tx_origin;
"""


def upgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
