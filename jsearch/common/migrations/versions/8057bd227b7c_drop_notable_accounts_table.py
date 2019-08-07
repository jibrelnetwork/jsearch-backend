"""Drop notable accounts table

Revision ID: 8057bd227b7c
Revises: 0d12cb1ee33d
Create Date: 2019-08-07 11:12:47.163225

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables


# revision identifiers, used by Alembic.
revision = '8057bd227b7c'
down_revision = '926bfa537c16'
branch_labels = None
depends_on = None


UP_SQL = """
DROP TABLE notable_accounts;
"""

DOWN_SQL = """
CREATE TABLE notable_accounts (
    address CHARACTER VARYING NOT NULL,
    name CHARACTER VARYING NOT NULL,
    labels CHARACTER VARYING[] NOT NULL
);

ALTER TABLE ONLY notable_accounts
    ADD CONSTRAINT notable_accounts_pkey PRIMARY KEY (address);
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)
