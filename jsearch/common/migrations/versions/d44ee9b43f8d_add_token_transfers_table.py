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
down_revision = '2b3b48c8762b'
branch_labels = None
depends_on = None



UP_SQL = """CREATE TABLE token_transfers (
                address varchar, 
                transaction_hash varchar,
                log_index integer,
                block_number integer,
                block_hash integer,
                timestamp integer,
                from_address varchar,
                to_address varchar,
                token_address varchar,
                token_value numeric,
                token_decimals integer,
                token_name varchar,
                token_symbol varchar,
                is_forked bool default false
                );
                
            CREATE INDEX ix_token_transfers_address_block_number_log_index ON token_transfers(address, block_number, log_index);
            CREATE INDEX ix_token_transfers_token_address_block_number_log_index ON token_transfers(token_address, block_number, log_index);
            CREATE INDEX ix_token_transfers_block_hash ON token_transfers(block_hash);
            """
DOWN_SQL = """
            DROP TABLE token_transfers;
            DROP INDEX ix_token_transfers_address_block_number_log_index;
            DROP INDEX ix_token_transfers_block_hash;
            """


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)