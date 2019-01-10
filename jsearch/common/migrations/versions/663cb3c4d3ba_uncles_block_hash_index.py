"""uncles block hash index

Revision ID: 663cb3c4d3ba
Revises: 8895b229311d
Create Date: 2018-12-27 10:11:13.653403

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '663cb3c4d3ba'
down_revision = '8895b229311d'
branch_labels = None
depends_on = None


UP_SQL = """CREATE INDEX IF NOT EXISTS ix_uncles_block_hash ON uncles USING btree (block_hash)"""
DOWN_SQL = """DROP INDEX ix_uncles_block_hash"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
