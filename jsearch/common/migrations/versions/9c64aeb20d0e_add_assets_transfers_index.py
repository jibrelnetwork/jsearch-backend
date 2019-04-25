"""add assets transfers index

Revision ID: 9c64aeb20d0e
Revises: 0bbb93975419
Create Date: 2019-04-24 13:29:08.589933

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9c64aeb20d0e'
down_revision = 'f7c9bc772fab'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE INDEX CONCURRENTLY ix_assets_summary ON assets_transfers (address, asset_address);
"""

DOWN_SQL = """
DROP INDEX CONCURRENTLY ix_assets_summary;
"""


def upgrade():
    op.execute('COMMIT')  # HACK: stop transaction and do action concurrently
    op.execute(UP_SQL)


def downgrade():
    op.execute("COMMIT")  # HACK: stop transaction and do action concurrently
    op.execute(DOWN_SQL)
