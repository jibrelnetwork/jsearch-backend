"""add balance request table

Revision ID: 9771b1bf1fae
Revises: b0aa5acc7400
Create Date: 2019-07-09 15:52:52.140193

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9771b1bf1fae'
down_revision = 'b0aa5acc7400'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE erc20_balance_requests (
    token_address CHARACTER VARYING NOT NULL,
    account_address CHARACTER VARYING NOT NULL,
    block_number INTEGER,
    balance NUMERIC
);

ALTER TABLE ONLY erc20_balance_requests
    ADD CONSTRAINT erc20_balance_requests_primary_key PRIMARY KEY (token_address, account_address);
"""

DOWN_SQL = """
DROP TABLE erc20_balance_requests;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
