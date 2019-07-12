"""remove_unused_indexes

Revision ID: 72ee769ee5e2
Revises: 9fca36eec5a5
Create Date: 2019-07-12 10:23:51.148881

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '72ee769ee5e2'
down_revision = '9fca36eec5a5'
branch_labels = None
depends_on = None


UP_SQL = """
DROP INDEX ix_logs_is_transfer_processed_multicolumn;
DROP INDEX ix_logs_is_token_transfer_multicolumn;
DROP INDEX ix_transactions_to;
DROP INDEX ix_transactions_from;
"""

DOWN_SQL = """
CREATE INDEX ix_logs_is_transfer_processed_multicolumn ON public.logs USING btree (is_processed, block_number);
CREATE INDEX ix_logs_is_token_transfer_multicolumn ON public.logs USING btree (is_token_transfer, is_transfer_processed, block_number);
CREATE INDEX ix_transactions_to ON public.transactions USING btree ("to");
CREATE INDEX ix_transactions_from ON public.transactions USING btree ("from");
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
