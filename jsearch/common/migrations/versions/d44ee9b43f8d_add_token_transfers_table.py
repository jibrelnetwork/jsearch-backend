"""add token_transfers table

Revision ID: d44ee9b43f8d
Revises: 663cb3c4d3ba
Create Date: 2019-01-15 13:00:36.488313

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = 'd44ee9b43f8d'
down_revision = '663cb3c4d3ba'
branch_labels = None
depends_on = None



UP_SQL = """CREATE TABLE token_transfers (
                address varchar, 
                transaction_hash varchar,
                log_index integer,
                block_number integer,
                timestamp integer,
                from_address varchar,
                to_address varchar,
                token_value numeric,
                token_name varchar,
                token_symbol varchar,
                );"""
DOWN_SQL = """ALTER TABLE reorgs DROP COLUMN node_id"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)