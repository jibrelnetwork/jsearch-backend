"""Add logs.address index

Revision ID: 9f4d8f8f7f24
Revises: 9e7a95e13e7d
Create Date: 2018-12-25 18:17:48.846048

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '9f4d8f8f7f24'
down_revision = '9e7a95e13e7d'
branch_labels = None
depends_on = None


UP_SQL = """CREATE INDEX IF NOT EXISTS ix_logs_address ON public.logs USING btree (address)"""
DOWN_SQL = """DROP INDEX ix_logs_address"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
