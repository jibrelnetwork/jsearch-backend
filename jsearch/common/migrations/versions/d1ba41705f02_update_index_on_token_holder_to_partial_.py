"""Update index on token holder to partial on is_forked

Revision ID: d1ba41705f02
Revises: dbc45a7dcac2
Create Date: 2019-08-22 10:47:42.099647

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'd1ba41705f02'
down_revision = '2e002bdb292f'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE token_holders ADD COLUMN block_hash VARCHAR;
ALTER TABLE token_holders ADD COLUMN is_forked BOOLEAN;
ALTER TABLE token_holders ALTER COLUMN is_forked SET DEFAULT false;

DROP INDEX IF EXISTS idx_th_block_number;
DROP INDEX IF EXISTS ix_token_holders_balance;
DROP INDEX IF EXISTS ix_token_holders_token_balance_id;
ALTER TABLE token_holders DROP CONSTRAINT IF EXISTS token_holders_pkey;
CREATE UNIQUE INDEX token_holders_by_block_hash on token_holders(block_hash, token_address, account_address);
"""

DOWN_SQL = """
DROP INDEX IF EXISTS token_holders_by_block_hash;
ALTER TABLE token_holders DROP COLUMN block_hash;
ALTER TABLE token_holders DROP COLUMN is_forked;
CREATE INDEX ix_token_holders_balance ON token_holders USING btree (balance);
CREATE INDEX idx_th_block_number ON token_holders USING btree (block_number);
CREATE INDEX ix_token_holders_token_balance_id ON token_holders USING btree (token_address, balance, id);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
