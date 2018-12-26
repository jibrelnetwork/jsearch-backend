import logging

from jsearch.common.celery import app

logger = logging.getLogger(__name__)


@app.task
def on_new_contracts_added_task(address):
    from jsearch.common.database import get_main_db
    logger.info('Starting process transactions for contract at %s', address)

    with get_main_db() as db:
        db.reset_processing_on_logs(contract_address=address)
    logger.info('%s transactions found for %s', address)
