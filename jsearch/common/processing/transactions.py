import logging
from typing import Any, Dict

from sqlalchemy.sql import select

from jsearch.common import contracts
from jsearch.common.integrations.contracts import get_contract
from jsearch.common.processing.logs import process_log_event
from jsearch.common.processing.erc20_transfer_logs import process_log_operations_bulk
from jsearch.common.tables import transactions_t

logger = logging.getLogger(__name__)


def process_transaction(receipt, tx_data) -> Dict[str, Any]:
    if tx_data['to'] == contracts.NULL_ADDRESS:
        contract_address = receipt['contractAddress']
    else:
        contract_address = tx_data['to']

    contract = get_contract(contract_address)
    if contract is not None:
        try:
            call = contracts.decode_contract_call(contract['abi'], tx_data['input'])
        except Exception as e:
            logger.warning(e)
            logger.exception('Call decode error: <%s>', tx_data)
        else:
            if call:
                tx_data['contract_call_description'] = call
    elif tx_data['to'] == contracts.NULL_ADDRESS:
        # ToDo: contract creation, check Transfer events
        pass
    return tx_data


def process_token_transfers_for_transaction(db, tx_hash: str) -> None:
    logger.info('Processing token transfer for TX %s', tx_hash)
    q = select([transactions_t]).where(transactions_t.c.hash == tx_hash)
    tx = db.conn.execute(q).fetchone()
    if tx is None:
        logger.warning('TX with hash %s not found', tx_hash)
        return

    with db.conn.begin():
        logs = db.get_transaction_logs(tx['hash'])
        if not logs:
            logger.info('No logs - no transfers')
            return

        if tx['to'] == contracts.NULL_ADDRESS:
            contract_address = logs[0]['address']
        else:
            contract_address = tx['to']

        contract = get_contract(contract_address)

        logs = [process_log_event(log) for log in logs]
        process_log_operations_bulk(db, logs, contract)
        for log in logs:
            db.update_log(log)
