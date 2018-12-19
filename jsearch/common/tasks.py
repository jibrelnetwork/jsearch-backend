import logging

from jsearch.common.celery import app

logger = logging.getLogger(__name__)


@app.task
def on_new_contracts_added_task(address, abi):
    from jsearch.common.operations import update_token_info
    from jsearch.common.processing.transactions import process_token_transfers_for_transaction
    from jsearch.common.database import get_main_db
    logger.info('Starting process transactions for contract at %s', address)
    update_token_info(address, abi)

    tx_count = 0
    with get_main_db() as db:
        for tx in db.get_contract_transactions(address):
            process_token_transfers_for_transaction(db, tx['hash'])
            tx_count += 1
    logger.info('%s transactions found for %s', tx_count, address)
