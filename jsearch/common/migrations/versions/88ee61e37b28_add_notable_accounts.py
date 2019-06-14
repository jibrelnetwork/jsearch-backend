"""Add notable accounts table.

Revision ID: 88ee61e37b28
Revises: c3b0dfc27a06
Create Date: 2019-06-06 11:10:03.473629

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '88ee61e37b28'
down_revision = 'c3b0dfc27a06'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE TABLE notable_accounts (
    address CHARACTER VARYING NOT NULL,
    name CHARACTER VARYING NOT NULL,
    labels CHARACTER VARYING[] NOT NULL
);

ALTER TABLE ONLY notable_accounts
    ADD CONSTRAINT notable_accounts_pkey PRIMARY KEY (address);
"""

DOWN_SQL = """
DROP TABLE notable_accounts;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
