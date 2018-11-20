import logging

from sqlalchemy.sql import select

from jsearch.common import contracts, tasks
from jsearch.common.database import MainDBSync
from jsearch.common.integrations.contracts import get_contract
from jsearch.common.processing.logs import process_logs
from jsearch.common.tables import transactions_t

logger = logging.getLogger(__name__)


def update_token_balance_bulk(need_update_token_balance) -> None:
    for token_address, account_address in need_update_token_balance:
        tasks.update_token_holder_balance_task.delay(token_address, account_address, None)


def update_token_info_bulk(need_update_token_info, abi=None) -> None:
    for address in need_update_token_info:
        tasks.on_new_contracts_added_task.delay(address, abi)


def process_token_transfers(db: MainDBSync, tx_hash: str) -> None:
    logger.info('Processing token transfer for TX %s', tx_hash)
    q = select([transactions_t]).where(transactions_t.c.hash == tx_hash)
    tx = db.conn.execute(q).fetchone()
    if tx is None:
        logger.warning('TX with hash %s not found', tx_hash)
        return

    with db.conn.begin():
        logs = db.get_transaction_logs(db.conn, tx['hash'])
        if not logs:
            logger.info('No logs - no transfers')
            return

        if tx['to'] == contracts.NULL_ADDRESS:
            contract_address = logs[0]['address']
        else:
            contract_address = tx['to']

        contract = get_contract(contract_address)
        logs, need_update_token_info, need_update_token_balance = process_logs(logs, contract)

        update_token_info_bulk(need_update_token_info)
        update_token_balance_bulk(need_update_token_balance)

        db.update_logs(db.conn, logs)
