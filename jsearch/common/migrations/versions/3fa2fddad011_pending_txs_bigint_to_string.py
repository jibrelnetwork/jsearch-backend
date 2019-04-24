"""pending txs bigint to numeric

Revision ID: 3fa2fddad011
Revises: eb4c04c8de9f
Create Date: 2019-04-12 15:04:24.244453

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '3fa2fddad011'
down_revision = '011a3c7b48e3'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE ONLY "pending_transactions" ALTER COLUMN "value" SET DATA TYPE CHARACTER VARYING;
"""

DOWN_SQL = """
ALTER TABLE ONLY "pending_transactions" DROP COLUMN "value";
ALTER TABLE ONLY "pending_transactions" ADD COLUMN "value" BIGINT;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
