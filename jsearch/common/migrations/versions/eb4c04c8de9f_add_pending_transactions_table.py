"""Add pending transactions table

Revision ID: eb4c04c8de9f
Revises: 63678873b8aa
Create Date: 2019-04-05 15:16:31.028616

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'eb4c04c8de9f'
down_revision = '63678873b8aa'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE pending_transactions (
    "last_synced_id" BIGINT NOT NULL,
    "hash" CHARACTER VARYING (70) NOT NULL,
    "status" CHARACTER VARYING,
    "timestamp" TIMESTAMP NOT NULL,
    "removed" BOOLEAN NOT NULL,
    "node_id" CHARACTER VARYING (70),
    "r" CHARACTER VARYING,
    "s" CHARACTER VARYING,
    "v" CHARACTER VARYING,
    "to" CHARACTER VARYING,
    "from" CHARACTER VARYING,
    "gas" BIGINT,
    "gas_price" BIGINT,
    "input" CHARACTER VARYING,
    "nonce" BIGINT,
    "value" BIGINT
);

ALTER TABLE ONLY pending_transactions ADD CONSTRAINT pending_transactions_pkey
     PRIMARY KEY (hash);
"""

DOWN_SQL = """
DROP TABLE pending_transactions;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
