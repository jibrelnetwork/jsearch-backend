"""Add timestamp for transactions

Revision ID: f50be7d3334d
Revises: 72ee769ee5e2
Create Date: 2019-07-23 20:52:12.317438

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'f50be7d3334d'
down_revision = '72ee769ee5e2'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE transactions ADD COLUMN "timestamp" integer;

UPDATE transactions SET "timestamp" = blocks.timestamp 
FROM blocks 
WHERE transactions.block_hash = blocks.hash;

CREATE INDEX IF NOT EXISTS ix_transactions_timestamp_index ON transactions("timestamp", transaction_index) 
WHERE is_forked = false; 
"""

DOWN_SQL = """
ALTER TABLE transactions DROP COLUMN "timestamp";
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
