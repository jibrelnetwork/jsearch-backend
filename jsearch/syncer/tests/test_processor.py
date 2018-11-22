from jsearch.syncer.processor import SyncProcessor


def test_syncer_write_block_data(db_connection_string, raw_db_connection_string, main_db_dump):
    p = SyncProcessor(raw_db_dsn=raw_db_connection_string, main_db_dsn=db_connection_string)
    p.sync_block(2)
