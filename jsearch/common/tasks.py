import logging

from jsearch.common.celery import app

logger = logging.getLogger(__name__)


@app.task
def on_new_contracts_added_task(address):
    from jsearch.common.database import get_main_db
    logger.info('Starting process transactions', extra={'contract': address})

    with get_main_db() as db:
        db.reset_processing_on_logs(contract_address=address)

    logger.info('transactions found for address', extra={'contract': address})
