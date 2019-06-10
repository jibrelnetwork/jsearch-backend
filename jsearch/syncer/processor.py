import logging
from collections import defaultdict

import re
import time
from typing import Optional, NamedTuple, Dict, Any, List, Tuple
from copy import copy

from jsearch import service_bus
from jsearch import settings
from jsearch.common import contracts

from jsearch.common.processing.logs import process_log_event
from jsearch.common.processing.erc20_transfers import logs_to_transfers_decimals
from jsearch.common.processing.erc20_balances import get_balances
from jsearch.syncer.database import RawDBAsync, MainDBAsync
from jsearch.typing import Logs
from jsearch.common.processing import wallet
from jsearch.common.processing.decimals_cache import decimals_cache


logger = logging.getLogger(__name__)


class BlockData(NamedTuple):
    block: Dict[str, Any]
    txs: List[Dict[str, Any]]
    logs: List[Dict[str, Any]]
    uncles: List[Dict[str, Any]]
    receipts: List[Dict[str, Any]]
    accounts: List[Dict[str, Any]]
    internal_txs: List[Dict[str, Any]]
    transfers: List[Dict[str, Any]]
    token_holders_updates: List[Dict[str, Any]]
    wallet_events: List[Dict[str, Any]]
    assets_summary_updates: List[Dict[str, Any]]

    async def write_to_database(self, main_db: MainDBAsync) -> None:
        await main_db.write_block_data_proc(
            block_data=self.block,
            uncles_data=self.uncles,
            transactions_data=self.txs,
            receipts_data=self.receipts,
            logs_data=self.logs,
            accounts_data=self.accounts,
            internal_txs_data=self.internal_txs,
            transfers=self.transfers,
            token_holders_updates=self.token_holders_updates,
            wallet_events=self.wallet_events,
            assets_summary_updates=self.assets_summary_updates
        )
        # await main_db.write_block_data_proc(
        #     block_data=self.block,
        #     uncles_data=[],
        #     transactions_data=[],
        #     receipts_data=[],
        #     logs_data=[],
        #     accounts_data=[],
        #     internal_txs_data=[]
        # )

    def write_to_bus(self):
        contracts_set = set()
        for acc in self.accounts:
            if acc['code'] != '':
                contracts_set.add(acc['address'])

        internal_tx_parent_map = defaultdict(list)
        for internal_tx in self.internal_txs:
            internal_tx_parent_map[internal_tx['parent_tx_hash']].append(internal_tx)

        tx_status_map = {receipt["transaction_hash"]: receipt["status"] for receipt in self.receipts}
        assert len(tx_status_map) == len(self.txs)

        for tx in self.txs:
            receipt_status = tx_status_map[tx['hash']]
            internal_txs = internal_tx_parent_map[tx['hash']]
            to_contract = tx['to'] in contracts_set
            tx.update(
                internal_transactions=internal_txs,
                receipt_status=receipt_status,
                to_contract=to_contract,
            )

            service_bus.sync_client.write_tx(tx)

        for account in self.accounts:
            service_bus.sync_client.write_account(account)

        logs = [dict(log, **{'status': tx_status_map[log['transaction_hash']]}) for log in self.logs]
        service_bus.sync_client.write_logs(logs=logs)


class SyncProcessor:
    """
    Raw-to-Main DB data sync processor
    """

    def __init__(self, raw_db_dsn: Optional[str] = None, main_db_dsn: Optional[str] = None):
        self.raw_db = RawDBAsync(raw_db_dsn or settings.JSEARCH_RAW_DB)
        self.main_db = MainDBAsync(main_db_dsn or settings.JSEARCH_MAIN_DB)

        #service_bus.sync_client.allow_rpc = False
        #service_bus.sync_client.start()

    async def sync_block(self, block_hash: str, block_number: int = None, is_forked: bool = False) -> bool:
        """
        :param block_number: number of block to sync
        :return: True if sync is successfull, False if syn fails or block already synced
        """
        logger.debug("Syncing Block", extra={'hash': block_hash, 'number': block_number})
        start_time = time.monotonic()
        await self.main_db.connect()
        await self.raw_db.connect()

        # self.raw_db.disconnect()
        # self.main_db.disconnect()
        # logger.info("Block is synced", extra={'sync_time': time.monotonic() - start_time})
        #
        # return


        start_time = time.monotonic()
        is_block_exist = await self.main_db.is_block_exist(block_hash)
        if is_block_exist is True:
            logger.debug("Block already exists", extra={'hash': block_hash})
            return False
        receipts = await self.raw_db.get_block_receipts(block_hash)
        if receipts is None:
            logger.debug("Block is not ready, no receipts", extra={'hash': block_hash})
            return False
        reward = await self.raw_db.get_reward(block_hash)
        if reward is None:
            logger.debug("Block is not ready, no reward", extra={'hash': block_hash})
            return False

        header = await self.raw_db.get_header_by_hash(block_hash)
        body = await self.raw_db.get_block_body(block_hash)
        accounts = await self.raw_db.get_block_accounts(block_hash)
        internal_transactions = await self.raw_db.get_internal_transactions(block_hash)
        fetch_time = time.monotonic() - start_time
        #self.raw_db.disconnect()

        block = await self.process_block(
            header=header,
            body=body,
            accounts=accounts,
            receipts=receipts,
            reward=reward,
            internal_transactions=internal_transactions,
            is_forked=is_forked
        )
        process_time = time.monotonic() - fetch_time - start_time

        await block.write_to_database(self.main_db)
        db_write_time = time.monotonic() - process_time - fetch_time - start_time
        #block.write_to_bus()
        bus_write_time = time.monotonic() - db_write_time - process_time - fetch_time - start_time

        sync_time = time.monotonic() - start_time
        logger.info("Block is synced", extra={
            'hash': block_hash,
            'number': block_number,
            'sync_time': '{:0.2f}s'.format(sync_time),
            'fetch_time': '{:0.2f}s'.format(fetch_time),
            'process_time': '{:0.2f}s'.format(process_time),
            'db_write_time': '{:0.2f}s'.format(db_write_time),
            'bus_write_time': '{:0.2f}s'.format(bus_write_time),
        })
        #await self.main_db.disconnect()
        return True

    async def process_block(self, header, body, reward, receipts, accounts, internal_transactions, is_forked) -> BlockData:
        """
        Preprocess data fetched from Raw DB to Main DB
        """
        uncles: List[Dict[str, Any]] = body['fields']['Uncles'] or []
        transactions: List[Dict[str, Any]] = body['fields']['Transactions'] or []
        block_number: int = header['block_number']
        block_hash: str = header['block_hash']

        block_reward, uncles_rewards = self.process_rewards(reward, block_number)
        block_data = self.process_header(header, block_reward, is_forked)
        uncles_data = self.process_uncles(uncles, uncles_rewards, block_number, block_hash, is_forked)
        transactions_data = self.process_transactions(transactions, block_number, block_hash, is_forked)
        receipts_data, logs_data = self.process_receipts(
            receipts=receipts,
            transactions=transactions_data,
            block_number=block_number,
            block_hash=block_hash,
            is_forked=is_forked
        )
        accounts_data = self.process_accounts(accounts, block_number, block_hash, is_forked)
        internal_txs_data = self.process_internal_txs(internal_transactions, is_forked)

        contracts_set = set()
        for acc in accounts_data:
            if acc['code'] != '':
                contracts_set.add(acc['address'])

        decimals = await decimals_cache.get_many({l['address'] for l in logs_data})
        transfers = logs_to_transfers_decimals(logs_data, block_data, decimals)

        token_holders_updates = await self.get_token_holders_updates(transfers)

        wallet_events = wallet.events_from_transactions(transactions_data, contracts_set)
        wallet_events.extend(wallet.events_from_transfers(transfers, transactions_data))
        wallet_events.extend(wallet.events_from_internal_transactions(internal_txs_data, transactions_data))
        wallet_events = [e for e in wallet_events if e is not None]

        assets_summary_updates = wallet.assets_from_accounts(accounts_data)
        assets_summary_updates.extend(wallet.assets_from_token_balance_updates(token_holders_updates))

        return BlockData(
            block=block_data,
            uncles=uncles_data,
            receipts=receipts_data,
            logs=logs_data,
            accounts=accounts_data,
            txs=transactions_data,
            internal_txs=internal_txs_data,
            transfers=transfers,
            token_holders_updates=token_holders_updates,
            wallet_events=wallet_events,
            assets_summary_updates=assets_summary_updates
        )

    def process_rewards(self,
                        reward: Dict[str, Any],
                        block_number: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        if block_number == 0:
            block_reward = {'static_reward': 0, 'uncle_inclusion_reward': 0, 'tx_fees': 0}
            uncles_rewards = []
        else:
            reward_data = reward['fields']
            block_reward = {
                'static_reward': reward_data['BlockReward'],
                'uncle_inclusion_reward': reward_data['UncleInclusionReward'],
                'tx_fees': reward_data['TxsReward']
            }
            uncles_rewards = reward_data['Uncles']
        return block_reward, uncles_rewards

    def process_header(self, header: Dict[str, Any], reward: Dict[str, Any], is_forked: bool) -> Dict[str, Any]:
        data = dict_keys_case_convert(header['fields'])
        data.update(reward)
        hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
        data['is_forked'] = is_forked
        if 'size' not in data:
            data['size'] = None
        if 'total_difficulty' not in data:
            data['total_difficulty'] = None
        data['is_sequence_sync'] = True
        return data

    def process_uncles(self,
                       uncles: List[Dict[str, Any]],
                       rewards: List[Dict[str, Any]],
                       block_number: int,
                       block_hash: str,
                       is_forked: bool) -> List[Dict[str, Any]]:
        items = []
        for i, uncle in enumerate(uncles):
            rwd = rewards[i]
            data = dict_keys_case_convert(uncle)
            assert rwd['UnclePosition'] == i
            data['reward'] = rwd['UncleReward']
            data['block_hash'] = block_hash
            data['block_number'] = block_number
            hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
            data['is_forked'] = is_forked
            if 'size' not in data:
                data['size'] = None
            if 'total_difficulty' not in data:
                data['total_difficulty'] = None
            items.append(data)
        return items

    def process_transactions(self, transactions, block_number, block_hash, is_forked):
        items = []
        for i, tx in enumerate(transactions):
            tx_data = dict_keys_case_convert(tx)
            tx_data['transaction_index'] = i
            tx_data['block_hash'] = block_hash
            tx_data['block_number'] = block_number
            tx_data['is_forked'] = is_forked
            if tx['to'] is None:
                tx_data['to'] = contracts.NULL_ADDRESS
            tx_data['address'] = tx_data['from']
            items.append(tx_data)
            tx2_data = copy(tx_data)
            tx2_data['address'] = tx_data['to']
            items.append(tx2_data)

        return items

    def process_receipts(self,
                         receipts: Dict[str, Any],
                         transactions: List[Dict[str, Any]],
                         block_number: int,
                         block_hash: str,
                         is_forked: bool) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        rdata: List[Dict[str, Any]] = receipts['fields']['Receipts'] or []
        recpt_items = []
        logs_items = []
        for i, receipt in enumerate(rdata):
            recpt_data = dict_keys_case_convert(receipt)
            tx = transactions[i*2]
            assert tx['hash'] == recpt_data['transaction_hash']

            recpt_data['transaction_index'] = i
            recpt_data['to'] = tx['to']
            recpt_data['from'] = tx['from']
            recpt_data.update({
                'transaction_index': i,
                'to': tx['to'],
                'from': tx['from'],
                'block_hash': block_hash,
                'block_number': block_number,
            })

            logs = recpt_data.pop('logs') or []
            recpt_data['block_hash'] = block_hash
            recpt_data['block_number'] = block_number
            recpt_data['is_forked'] = is_forked
            hex_vals_to_int(recpt_data, ['cumulative_gas_used', 'gas_used', 'status'])
            recpt_items.append(recpt_data)
            tx['status'] = recpt_data['status']
            transactions[i * 2 + 1]['status'] = recpt_data['status']
            logs = self.process_logs(logs, status=recpt_data['status'], is_forked=is_forked)
            logs_items.extend(logs)
        return recpt_items, logs_items

    def process_logs(self, logs: Logs, status: bool, is_forked: bool) -> Logs:
        items = []
        for log_record in logs:
            data = dict_keys_case_convert(log_record)
            hex_vals_to_int(data, ['log_index', 'transaction_index', 'block_number'])
            data['is_token_transfer'] = False
            data['token_transfer_to'] = None
            data['token_transfer_from'] = None
            data['token_amount'] = None
            data['event_type'] = None
            data['event_args'] = None
            data['status'] = status
            data['is_forked'] = is_forked
            data = process_log_event(data)
            items.append(data)
        return items

    def process_accounts(self,
                         accounts: List[Dict[str, Any]],
                         block_number: int,
                         block_hash: str,
                         is_forked: bool) -> List[Dict[str, Any]]:
        items = []
        for account in accounts:
            data = dict_keys_case_convert(account['fields'])
            data['address'] = account['address'].lower()
            data['block_number'] = block_number
            data['block_hash'] = block_hash
            data['is_forked'] = is_forked
            items.append(data)
        return items

    def process_internal_txs(self, internal_txs: List[Dict[str, Any]], is_forked: bool) -> List[Dict[str, Any]]:
        items = []
        for i, tx in enumerate(internal_txs, 1):
            data = dict_keys_case_convert(tx['fields'])
            data['timestamp'] = data.pop('time_stamp')
            data['transaction_index'] = i
            del data['operation']
            data['op'] = tx['type']
            data['is_forked'] = is_forked
            items.append(data)
        return items

    async def get_token_holders_updates(self, transfers):
        holders = set()
        for t in transfers:
            to_address = t['to_address']
            from_address = t['from_address']
            token_address = t['token_address']
            if to_address != contracts.NULL_ADDRESS:
                holders.add((to_address, token_address))
            if from_address != contracts.NULL_ADDRESS:
                holders.add((from_address, token_address))

        balances = await get_balances(list(holders), 20)
        updates = []
        for b in balances:
            update = {
                'account_address': b[0],
                'token_address': b[1],
                'balance': b[2],
                'decimals': 18  # FIXME!!!!! use real decimals
            }
            updates.append(update)
        return updates


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
