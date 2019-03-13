from functools import partial
from itertools import chain, count
from typing import Dict, List

from jsearch import settings
from jsearch.async_utils import do_parallel
from jsearch.common.rpc import ContractCall, eth_call_batch
from jsearch.service_bus import service_bus
from jsearch.typing import Contract, Contracts
from jsearch.utils import split


def prefetch_decimals(contracts: List[Contract]) -> Dict[str, Contract]:
    contracts = chain(
        *(fetch_erc20_token_decimal_bulk(chunk) for chunk in split(contracts, settings.ETH_NODE_BATCH_REQUEST_SIZE))
    )
    return {contract['address']: contract for contract in contracts}


def fetch_erc20_token_decimal_bulk(contracts: Contracts) -> Contracts:
    calls = []
    counter = count()
    for contract in contracts:
        call = ContractCall(
            pk=next(counter),
            abi=contract['abi'],
            address=contract['address'],
            method='decimals',
            silent=True
        )
        calls.append(call)

    calls_results = eth_call_batch(calls=calls)
    for call, contract in zip(calls, contracts):
        contract['decimals'] = calls_results.get(call.pk)

    return contracts


async def fetch_contracts(addresses: List[str]) -> Contracts:
    contracts = chain(
        *await do_parallel(
            func=partial(service_bus.get_contracts, fields=["address", "abi", ]),
            argument_list=addresses,
            chunk_size=settings.ETH_NODE_BATCH_REQUEST_SIZE
        )
    )
    return list(contracts)
