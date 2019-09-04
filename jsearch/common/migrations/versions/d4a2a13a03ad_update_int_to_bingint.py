"""Update int to bingint

Revision ID: d4a2a13a03ad
Revises: 1c8fb14439fd
Create Date: 2019-09-03 09:26:41.193290

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'd4a2a13a03ad'
down_revision = '1c8fb14439fd'
branch_labels = None
depends_on = None

# WTF: Postgres integer limit is 2,147,483,647. Soon or later we will reach this limit.
UP_SQL = """
ALTER TABLE accounts_state ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE assets_summary ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE blocks ALTER COLUMN number TYPE BIGINT;
ALTER TABLE transactions ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE internal_transactions ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE logs ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE reorgs ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE reorgs ALTER COLUMN id TYPE BIGINT;
ALTER TABLE token_holders ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE token_transfers ALTER COLUMN block_number TYPE BIGINT;
ALTER TABLE uncles ALTER COLUMN block_number TYPE BIGINT;
"""

DOWN_SQL = """
ALTER TABLE accounts_state ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE assets_summary ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE blocks ALTER COLUMN number TYPE INTEGER;
ALTER TABLE transactions ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE internal_transactions ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE logs ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE reorgs ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE reorgs ALTER COLUMN id TYPE INTEGER;
ALTER TABLE token_holders ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE token_transfers ALTER COLUMN block_number TYPE INTEGER;
ALTER TABLE uncles ALTER COLUMN block_number TYPE INTEGER;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
