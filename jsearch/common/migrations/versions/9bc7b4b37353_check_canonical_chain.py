"""check_canonical_chain

Revision ID: 9bc7b4b37353
Revises: 8057bd227b7c
Create Date: 2019-08-13 13:17:51.348621

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '9bc7b4b37353'
down_revision = '8057bd227b7c'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE OR REPLACE FUNCTION check_canonical_chain(depth integer)
  RETURNS TABLE(number bigint, hash varchar, parent_hash varchar) AS
$func$
DECLARE
   last_hash varchar;
BEGIN

FOR number, hash, parent_hash IN
   SELECT b.number, b.hash, b.parent_hash
   FROM   blocks b
   WHERE  b.is_forked = false AND b.number > ((SELECT MAX(bb.number) FROM blocks bb) - depth)
   ORDER  BY b.number ASC
LOOP
   IF last_hash <> parent_hash THEN
        RETURN NEXT;
   END IF;
   last_hash := hash;
END LOOP;

END
$func$ LANGUAGE plpgsql STABLE;
"""

DOWN_SQL = """
DROP FUNCTION check_canonical_chain(integer);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
