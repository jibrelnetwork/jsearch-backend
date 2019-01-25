"""add token_transfers table

Revision ID: d44ee9b43f8d
Revises: 663cb3c4d3ba
Create Date: 2019-01-15 13:00:36.488313

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'd44ee9b43f8d'
down_revision = '2b3b48c8762b'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE token_transfers 
  ( 
     address          VARCHAR, 
     transaction_hash VARCHAR, 
     log_index        INTEGER, 
     block_number     INTEGER, 
     block_hash       CHARACTER VARYING NOT NULL,
     timestamp        INTEGER, 
     from_address     VARCHAR, 
     to_address       VARCHAR, 
     token_address    VARCHAR, 
     token_value      NUMERIC, 
     token_decimals   INTEGER, 
     token_name       VARCHAR, 
     token_symbol     VARCHAR, 
     is_forked        BOOL DEFAULT FALSE 
  ); 

CREATE INDEX ix_token_transfers_address_block_number_log_index 
ON token_transfers(address, block_number, log_index); 

CREATE INDEX ix_token_transfers_token_address_block_number_log_index 
  ON token_transfers(token_address, block_number, log_index); 

CREATE INDEX ix_token_transfers_block_hash 
  ON token_transfers(block_hash); 
"""

DOWN_SQL = "DROP TABLE token_transfers;"


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
