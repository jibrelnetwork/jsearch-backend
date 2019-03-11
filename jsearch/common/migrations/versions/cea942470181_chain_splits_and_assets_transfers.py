"""chain_splits and assets_transfers

Revision ID: cea942470181
Revises: 78d176256d74
Create Date: 2019-02-19 07:28:00.195221

"""
from alembic import op
import sqlalchemy as sa
from jsearch.common import tables
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'cea942470181'
down_revision = '78d176256d74'
branch_labels = None
depends_on = None


UP_SQL = """
CREATE TABLE chain_splits (
    id bigserial PRIMARY KEY,
    common_block_number bigint,
    common_block_hash varchar,
    drop_length integer,
    drop_block_hash varchar,
    add_length integer,
    add_block_hash varchar,
    node_id varchar
);

ALTER TABLE reorgs ADD COLUMN split_id bigint DEFAULT null;
CREATE INDEX ix_reorgs_split_id ON reorgs(split_id);


CREATE TABLE assets_transfers (
    address varchar,
    type varchar,
    "from" varchar,
    "to" varchar,
    asset_address varchar,
    amount NUMERIC,
    tx_data jsonb,
    is_forked boolean,
    block_number integer,
    block_hash varchar,
    ordering integer
);

CREATE INDEX ix_assets_transfers ON assets_transfers (address, asset_address, is_forked, ordering);


CREATE TABLE assets_summary (
    address varchar,
    asset_address varchar,
    tx_number integer,
    balance NUMERIC,
    nonce integer
);

CREATE INDEX ix_assets_summary ON assets_transfers (address, asset_address);


ALTER TABLE transactions ADD COLUMN address varchar default '';
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
"""


DOWN_SQL = """
DROP TABLE chain_splits;
DROP TABLE assets_transfers;
DROP TABLE assets_summary;
ALTER TABLE transactions DROP COLUMN address;
"""


def upgrade():
    op.execute(UP_SQL)


def downgrade():
    op.execute(DOWN_SQL)

