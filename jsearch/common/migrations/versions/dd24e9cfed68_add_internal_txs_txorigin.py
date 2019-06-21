"""Add `internal_transactions.tx_origin`.

Revision ID: dd24e9cfed68
Revises: 88ee61e37b28
Create Date: 2019-06-16 13:18:31.480212

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = 'dd24e9cfed68'
down_revision = 'b6c8915186f8'
branch_labels = None
depends_on = None


UP_SQL = """
ALTER TABLE internal_transactions ADD COLUMN tx_origin CHARACTER VARYING;
CREATE INDEX CONCURRENTLY ix_internal_transactions_tx_origin ON internal_transactions("tx_origin");
"""

DOWN_SQL = """
ALTER TABLE internal_transactions DROP COLUMN tx_origin;
DROP INDEX ix_internal_transactions_tx_origin;
"""


def upgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
