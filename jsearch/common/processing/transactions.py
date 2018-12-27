import logging
from typing import Any, Dict

from jsearch.common import contracts
from jsearch.common.integrations.contracts import get_contract

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
