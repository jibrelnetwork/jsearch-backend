"""Fix missed token holders

Revision ID: 6f502a00541d
Revises: 1c8fb14439fd
Create Date: 2019-08-29 13:57:54.909465

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '6f502a00541d'
down_revision = '6a924e0a5f2b'
branch_labels = None
depends_on = None

UP_SQL = """
INSERT INTO token_holders (account_address, token_address, balance, decimals, block_number) 
    SELECT a.address, a.asset_address, a.value, a.decimals, a.block_number 
    FROM assets_summary AS a 
        LEFT JOIN token_holders AS h ON a.address = h.account_address AND a.asset_address = h.token_address 
    WHERE h.account_address IS NULL AND a.asset_address != '' AND a.block_number IS NOT NULL
ON CONFLICT (account_address, token_address) DO NOTHING;
"""

DOWN_SQL = """
SELECT 1;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
