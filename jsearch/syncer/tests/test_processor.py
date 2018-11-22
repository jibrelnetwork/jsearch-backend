import os

from jsearch.common import tables as t
from jsearch.syncer.processor import SyncProcessor


def test_maindb_write_block_data(db, main_db_dump):
    p = SyncProcessor()
    p.sync_block(2)
