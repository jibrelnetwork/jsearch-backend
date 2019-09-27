"""token_holders_clean_procedure

Revision ID: 6cbbf0051aa0
Revises: b64000d69819
Create Date: 2019-09-27 08:59:23.547040

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '6cbbf0051aa0'
down_revision = 'b64000d69819'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE OR REPLACE FUNCTION clean_holder(token varchar)
  RETURNS integer  AS 
$func$
DECLARE
   last_hash varchar;
   acc RECORD;
BEGIN

FOR acc IN
   select distinct account_address from token_holders where token_address = token
LOOP
   delete from token_holders h
    where h.token_address = token
        and h.account_address = acc.account_address
        and h.block_number < (select max(block_number) - 6 from token_holders hh
                                where hh.token_address = token
                                and hh.account_address = acc.account_address
                                and hh.is_forked=false);
END LOOP;
RETURN 1;
END
$func$ LANGUAGE plpgsql VOLATILE;
"""

DOWN_SQL = """
DROP FUNCTION IF EXISTS clean_holder(token varchar);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
