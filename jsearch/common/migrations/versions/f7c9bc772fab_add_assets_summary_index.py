"""add assets summary index

Revision ID: f7c9bc772fab
Revises: 0bbb93975419
Create Date: 2019-04-24 13:12:50.416794

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'f7c9bc772fab'
down_revision = 'cea942470181'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY ix_assets_transfers ON assets_transfers (address, asset_address, is_forked, ordering);
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_assets_transfers;
"""


def upgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
