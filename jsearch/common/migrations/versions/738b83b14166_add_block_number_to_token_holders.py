"""add block number to token holders

Revision ID: 738b83b14166
Revises: b0aa5acc7400
Create Date: 2019-07-04 09:06:07.844562

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '738b83b14166'
down_revision = 'cdec69ddbe53'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE token_holders ADD COLUMN block_number INTEGER;
CREATE INDEX idx_th_block_number on token_holders(block_number);
"""

DOWN_SQL = """
ALTER TABLE token_holders DROP COLUMN block_number
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
