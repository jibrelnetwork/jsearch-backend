"""add reorg autoincrement pk

Revision ID: 3cb8028b6494
Revises: 63678873b8aa
Create Date: 2019-04-10 15:36:03.270936

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '3cb8028b6494'
down_revision = '63678873b8aa'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE SEQUENCE reorgs_id_seq;
ALTER TABLE reorgs ALTER COLUMN id set DEFAULT nextval('reorgs_id_seq');
ALTER TABLE reorgs ADD COLUMN raw_id bigint NOT NULL;
"""

DOWN_SQL = """
ALTER TABLE reorgs ALTER COLUMN id DROP DEFAULT;
ALTER TABLE reorgs DROP COLUMN raw_id;
DROP SEQUENCE reorgs_id_seq;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
