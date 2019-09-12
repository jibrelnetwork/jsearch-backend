"""Add index by reorgs hash

Revision ID: 562a23392b0b
Revises: 554ff20408a1
Create Date: 2019-07-30 22:31:32.433225

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '562a23392b0b'
down_revision = '0e256b6ceec4'
branch_labels = None
depends_on = None

UP_SQL = """
select pg_sleep(10800);
ALTER TABLE reorgs DROP COLUMN "id";
ALTER TABLE reorgs ADD COLUMN "id" SERIAL;
CREATE UNIQUE INDEX ix_reorgs_hash_split_id_node_id ON reorgs(block_hash, split_id, node_id); 
"""

DOWN_SQL = """
DROP INDEX ix_reorgs_hash_split_id_node_id;
ALTER TABLE reorgs DROP COLUMN "id";
ALTER TABLE reorgs ADD COLUMN "id" BIGINT;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
