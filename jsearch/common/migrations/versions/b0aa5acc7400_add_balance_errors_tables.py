"""Add balance errors tables

Revision ID: b0aa5acc7400
Revises: cdec69ddbe53
Create Date: 2019-07-03 16:20:29.039946

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'b0aa5acc7400'
down_revision = '4a5aba9956c7'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE TABLE erc20_errors (
    contract_address CHARACTER VARYING NOT NULL,
    account_address CHARACTER VARYING NOT NULL,
    errors INTEGER DEFAULT 0,
    block_number INTEGER 
);

ALTER TABLE ONLY erc20_errors
    ADD CONSTRAINT erc20_errors_primary_key PRIMARY KEY (contract_address, account_address);
"""

DOWN_SQL = """
DROP TABLE erc20_errors;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
