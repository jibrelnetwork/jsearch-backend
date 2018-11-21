import logging

from jsearch.common.celery import app

logger = logging.getLogger(__name__)


# @app.task
# def process_new_verified_contract_transactions(address):
#     from jsearch.common.database import get_main_db
#     logger.info('Starting process_new_verified_contract_transactions for address %s', address)
#     db = get_main_db()
#     c = db.call_sync(db.get_contract(address))
#     if c is None:
#         logger.info('Contract %s is missed in DB', address)
#         return
#     try:
#         update_token_info(address, db)
#         tx_count = 0
#         for tx in db.call_sync(db.get_contract_transactions(address)):
#             process_token_transfer.delay(tx)
#             tx_count += 1
#         logger.info('%s transactions found for %s', tx_count, address)
#     finally:
#         db.call_sync(db.disconnect())


@app.task
def update_token_holder_balance_task(token_address, account_address, block_number=None):
    from jsearch.common.database import get_main_db
    from jsearch.common.processing.operations import update_token_holder_balance
    with get_main_db() as db:
        update_token_holder_balance(db, token_address, account_address, block_number)


@app.task
def on_new_contracts_added_task(address, abi):
    from jsearch.common.processing.operations import update_token_info
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
