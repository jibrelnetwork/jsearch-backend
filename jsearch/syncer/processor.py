import logging
import re
import time
from copy import copy
from typing import Dict, Any, List, Tuple

from jsearch.common import contracts
from jsearch.common.processing import wallet
from jsearch.common.processing.contracts_addresses_cache import contracts_addresses_cache
from jsearch.common.processing.erc20_transfers import logs_to_transfers
from jsearch.common.processing.logs import process_log_event
from jsearch.common.processing.wallet import token_holders_from_token_balances
from jsearch.common.utils import timeit, unique
from jsearch.syncer.database import MainDB
from jsearch.syncer.structs import RawBlockData, BlockData
from jsearch.typing import Logs, AnyDict

logger = logging.getLogger(__name__)

EXCLUDE_TX_ORIGIN = '0x8999999999999999999999999999999999999998'  # fake address used by fork to get token balances


def update_is_forked_state(items: List[Dict[str, Any]], is_forked: bool) -> List[Dict[str, Any]]:
    for item in items:
        item['is_forked'] = is_forked
    return items


@timeit('[CPU/GETH/MAIN DB] Process block')
async def process_block(main_db: MainDB, data: RawBlockData) -> BlockData:
    block_reward, uncles_rewards = process_rewards(data.reward, data.block_number)
    block_data = process_header(data.header, block_reward, data.transactions, data.uncles, data.is_forked)

    block_info = {
        'block_number': data.block_number,
        'block_hash': data.block_hash,
        'is_forked': data.is_forked,
    }

    uncles = process_uncles(data.uncles, uncles_rewards, **block_info)
    txs_data = process_txs(data.transactions, data.timestamp, **block_info)
    internal_txs = process_internal_txs(data.internal_txs, txs_data, data.is_forked)

    accounts = process_accounts(data.accounts, **block_info)
    receipts = process_receipts(data.receipts, txs_data, **block_info)

    logs = process_logs(data.receipts, data.timestamp, data.is_forked)

    contracts_set = set()
    for acc in accounts:
        if acc.get('code', '') != '':
            contracts_set.add(acc['address'])

    contracts_addresses_cache.add(contracts_set)

    tx_recipients = set([tx['to'] for tx in txs_data if tx['input'] != '0x'])
    contracts_set |= await get_contracts_set(main_db, tx_recipients)

    decimals = {balance.token: balance.decimals or 0 for balance in data.token_balances}
    transfers = logs_to_transfers(logs, block_data, decimals)

    wallet_events = [
        *wallet.events_from_transactions(txs_data, contracts_set=contracts_set),
        *wallet.events_from_transfers(transfers, txs_data),
        *wallet.events_from_internal_transactions(internal_txs, txs_data),
    ]
    wallet_events = [event for event in wallet_events if event is not None]
    wallet_events = update_is_forked_state(wallet_events, data.is_forked)

    # `assets_summary_pairs_updates` are fetched from token balances only by
    # design. Ether pairs are not stored in the `assets_summary_pairs` table.
    assets_summary_updates, assets_summary_pairs = wallet.assets_and_pairs_from_token_balances(
        token_holder_balances=data.token_balances,
        decimals_map=decimals,
    )

    assets_summary_updates = wallet.assets_from_accounts(accounts) + assets_summary_updates
    assets_summary_updates = update_is_forked_state(assets_summary_updates, is_forked=data.is_forked)

    assets_summary_pairs = unique(assets_summary_pairs)
    assets_summary_pairs = [pair.to_dict() for pair in assets_summary_pairs]

    token_holders_balances = token_holders_from_token_balances(data.token_balances, decimals_map=decimals)
    token_holders_balances = update_is_forked_state(token_holders_balances, is_forked=data.is_forked)

    return BlockData(
        block=block_data,
        uncles=uncles,
        receipts=receipts,
        logs=logs,
        accounts=accounts,
        txs=txs_data,
        internal_txs=internal_txs,
        transfers=transfers,
        token_holders_updates=token_holders_balances,
        wallet_events=wallet_events,
        assets_summary_updates=assets_summary_updates,
        assets_summary_pairs=assets_summary_pairs,
    )


@timeit("[CPU] Process rewards")
def process_rewards(
        reward: Dict[str, Any],
        block_number: int
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    if block_number == 0:
        block_reward = {'static_reward': 0, 'uncle_inclusion_reward': 0, 'tx_fees': 0}
        uncles_rewards: List[Dict[str, Any]] = []
    else:
        reward_data = reward['fields']
        block_reward = {
            'static_reward': reward_data['BlockReward'],
            'uncle_inclusion_reward': reward_data['UncleInclusionReward'],
            'tx_fees': reward_data['TxsReward']
        }
        uncles_rewards = reward_data['Uncles']
    return block_reward, uncles_rewards


@timeit("[CPU] Process headers")
def process_header(
        header: Dict[str, Any],
        reward: Dict[str, Any],
        txs: List[Dict[str, Any]],
        uncles: List[Dict[str, Any]],
        is_forked: bool
) -> Dict[str, Any]:
    data = dict_keys_case_convert(header['fields'])
    data.update(reward)
    hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])

    if 'size' not in data:
        data['size'] = None

    if 'total_difficulty' not in data:
        data['total_difficulty'] = None

    data.update(
        is_forked=is_forked,
        is_sequence_sync=True,
        transactions=[tx['hash'] for tx in txs],
        uncles=[uncle['hash'] for uncle in uncles],
        logs_bloom=''
    )
    return data


@timeit("[CPU] Process uncles")
def process_uncles(
        uncles: List[Dict[str, Any]],
        rewards: List[Dict[str, Any]],
        block_number: int,
        block_hash: str,
        is_forked: bool
) -> List[Dict[str, Any]]:
    items = []
    for i, uncle in enumerate(uncles):
        rwd = rewards[i]
        data = dict_keys_case_convert(uncle)
        assert rwd['UnclePosition'] == i

        data['reward'] = rwd['UncleReward']
        data['block_hash'] = block_hash
        data['block_number'] = block_number
        data['is_forked'] = is_forked
        data['logs_bloom'] = ''

        hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])

        if 'size' not in data:
            data['size'] = None

        if 'total_difficulty' not in data:
            data['total_difficulty'] = None

        items.append(data)
    return items


@timeit("[CPU] Process transactions")
def process_txs(
        transactions: List[Dict[str, Any]],
        timestamp: int,
        block_number: int,
        block_hash: str,
        is_forked: bool
) -> List[Dict[str, Any]]:
    items = []
    for i, tx in enumerate(transactions):
        tx_data = dict_keys_case_convert(tx)
        tx_data['transaction_index'] = i
        tx_data['block_hash'] = block_hash
        tx_data['block_number'] = block_number
        tx_data['is_forked'] = is_forked
        tx_data['timestamp'] = timestamp

        if tx['to'] is None:
            tx_data['to'] = contracts.NULL_ADDRESS

        tx_data['address'] = tx_data['from']

        items.append(tx_data)

        tx2_data = copy(tx_data)
        tx2_data['address'] = tx_data['to']
        items.append(tx2_data)

    return items


@timeit("[CPU] Process receipts")
def process_receipts(
        receipts: Dict[str, Any],
        transactions: List[Dict[str, Any]],
        block_number: int,
        block_hash: str,
        is_forked: bool
) -> List[Dict[str, Any]]:
    raw: List[Dict[str, Any]] = receipts['fields']['Receipts'] or []
    items = []
    for i, receipt in enumerate(raw):
        receipt = dict_keys_case_convert(receipt)

        tx = transactions[i * 2]
        assert tx['hash'] == receipt['transaction_hash']

        receipt.pop('logs')
        receipt.update({
            'transaction_index': i,
            'to': tx['to'],
            'from': tx['from'],
            'block_hash': block_hash,
            'block_number': block_number,
            'logs_bloom': '',
            'is_forked': is_forked
        })

        hex_vals_to_int(receipt, ['cumulative_gas_used', 'gas_used', 'status'])
        items.append(receipt)

        tx['status'] = receipt['status']
        transactions[i * 2 + 1]['status'] = receipt['status']
    return items


def process_logs(receipts: AnyDict, timestamp: int, is_forked: bool) -> Logs:
    items = []
    for receipt in receipts['fields']['Receipts'] or []:
        status = int(receipt['status'], 16)
        logs = receipt.get('logs') or []
        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            hex_vals_to_int(data, ['log_index', 'transaction_index', 'block_number'])
            data.update({
                'is_token_transfer': False,
                'token_transfer_to': None,
                'token_transfer_from': None,
                'token_amount': None,
                'event_type': None,
                'event_args': None,
                'status': status,
                'is_forked': is_forked,
                'timestamp': timestamp,
            })
            data = process_log_event(data)
            items.append(data)
    return items


@timeit("[CPU] Process accounts")
def process_accounts(
        accounts: List[Dict[str, Any]],
        block_number: int,
        block_hash: str,
        is_forked: bool
) -> List[Dict[str, Any]]:
    items = []
    for account in accounts:
        data = dict_keys_case_convert(account['fields'])
        data['address'] = account['address'].lower()
        data['block_number'] = block_number
        data['block_hash'] = block_hash
        data['is_forked'] = is_forked
        items.append(data)
    return items


@timeit("[CPU] Process internal transactions")
def process_internal_txs(internal_txs: List[Dict[str, Any]],
                         transactions: List[Dict[str, Any]],
                         is_forked: bool) -> List[Dict[str, Any]]:
    items = []
    tx_index_map = {t['hash']: t['transaction_index'] for t in transactions}
    for tx in internal_txs:
        data = dict_keys_case_convert(tx['fields'])
        if data['tx_origin'] == EXCLUDE_TX_ORIGIN:
            continue

        del data['operation']
        data.update({
            'op': tx['type'],
            'is_forked': is_forked,
            'timestamp': data.pop('time_stamp'),
            'transaction_index': tx['index'],
            'parent_tx_index': tx_index_map[tx['parent_tx_hash']]
        })

        items.append(data)
    return items


async def get_contracts_set(main_db: MainDB, addresses):
    misses = contracts_addresses_cache.get_misses(addresses)
    hits = contracts_addresses_cache.get_hits(addresses)
    start_t = time.time()
    new_entries = set(await main_db.is_contract_address(misses))
    end_t = time.time() - start_t
    logger.debug("Contract Address Cache Summary: misses %s hits %s new_entries %s db_time %0.2fs",
                 len(misses), len(hits), len(new_entries), end_t)
    if end_t > 0.1:
        logger.debug("Contract Misses: %s", misses)
    contracts_addresses_cache.add(new_entries)
    return hits | new_entries


first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def case_convert(name):
    s1 = first_cap_re.sub(r'\1_\2', name)
    return all_cap_re.sub(r'\1_\2', s1).lower()


def dict_keys_case_convert(d):
    return {case_convert(k): v for k, v in d.items()}


def hex_vals_to_int(d, keys):
    for k in keys:
        d[k] = int(d[k], 16)
