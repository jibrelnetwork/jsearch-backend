"""empty message

Revision ID: 78d176256d74
Revises: d44ee9b43f8d
Create Date: 2019-01-21 10:28:06.952792

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '78d176256d74'
down_revision = '4377c859182d'
branch_labels = None
depends_on = None

UP_SQL = """
ALTER TABLE ONLY token_transfers ADD CONSTRAINT token_transfers_unique 
    UNIQUE (transaction_hash, transaction_index, address);
"""
DOWN_SQL = "ALTER TABLE ONLY token_transfers DROP CONSTRAINT token_transfers_unique;"


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
