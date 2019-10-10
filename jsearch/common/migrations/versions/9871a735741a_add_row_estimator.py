"""Add row_estimator

Revision ID: 9871a735741a
Revises: 538fca58b088
Create Date: 2019-10-10 11:59:38.345263

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = '9871a735741a'
down_revision = '538fca58b088'
branch_labels = None
depends_on = None

UP_SQL = """
CREATE OR REPLACE FUNCTION row_estimator(query text) RETURNS bigint
   LANGUAGE plpgsql AS
$$DECLARE
   plan jsonb;
BEGIN
   EXECUTE 'EXPLAIN (FORMAT JSON) ' || query INTO plan;
 
   RETURN (plan->0->'Plan'->>'Plan Rows')::bigint;
END;$$;
"""

DOWN_SQL = """
DROP FUNCTION row_estimator;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
