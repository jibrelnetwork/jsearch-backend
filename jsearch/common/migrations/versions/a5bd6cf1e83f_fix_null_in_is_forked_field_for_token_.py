"""Fix null in is_forked field for token_holder and assets_summary

Revision ID: a5bd6cf1e83f
Revises: 1c8fb14439fd
Create Date: 2019-08-29 12:35:16.895126

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'a5bd6cf1e83f'
down_revision = 'd1ba41705f02'
branch_labels = None
depends_on = None

UP_SQL = """
UPDATE token_holders SET is_forked = false WHERE is_forked IS NULL;
UPDATE assets_summary SET is_forked = false WHERE is_forked IS NULL;

ALTER TABLE token_holders ALTER COLUMN is_forked SET NOT NULL;
ALTER TABLE assets_summary ALTER COLUMN is_forked SET NOT NULL;
"""

DOWN_SQL = """
SELECT 1;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
