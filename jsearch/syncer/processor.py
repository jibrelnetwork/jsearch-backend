import logging
import re
import time
from typing import Optional

from jsearch import settings
from jsearch.common import contracts
from jsearch.syncer.database import MainDBSync, RawDBSync

logger = logging.getLogger(__name__)


class SyncProcessor:
    """
    Raw-to-Main DB data sync processor
    """

    def __init__(self, raw_db_dsn: Optional[str] = None, main_db_dsn: Optional[str] = None):
        self.raw_db = RawDBSync(raw_db_dsn or settings.JSEARCH_RAW_DB)
        self.main_db = MainDBSync(main_db_dsn or settings.JSEARCH_MAIN_DB)

    def sync_block(self, block_number: int) -> bool:
        """
        :param block_number: number of block to sync
        :return: True if sync is successfull, False if syn fails or block already synced
        """

        logger.debug("Syncing Block #%s", block_number)

        self.main_db.connect()
        self.raw_db.connect()

        start_time = time.monotonic()
        is_block_exist = self.main_db.is_block_exist(block_number)
        if is_block_exist is True:
            logger.debug("Block #%s exist", block_number)
            return False
        receipts = self.raw_db.get_block_receipts(block_number)
        if receipts is None:
            logger.debug("Block #%s not ready: no receipts", block_number)
            return False

        header = self.raw_db.get_header_by_hash(block_number)
        body = self.raw_db.get_block_body(block_number)
        accounts = self.raw_db.get_block_accounts(block_number)
        reward = self.raw_db.get_reward(block_number)
        internal_transactions = self.raw_db.get_internal_transactions(block_number)

        self.write_block(header=header, body=body, accounts=accounts,
                         receipts=receipts, reward=reward,
                         internal_transactions=internal_transactions)

        sync_time = time.monotonic() - start_time
        logger.debug("Block #%s synced on %ss", block_number, sync_time)
        self.main_db.disconnect()
        self.raw_db.disconnect()
        return True

    def write_block(self, header, body, reward, receipts, accounts, internal_transactions):
        """
        Preprocess and write block data fetched from Raw DB to Main DB

        :param header:
        :param body:
        :param reward:
        :param receipts:
        :param accounts:
        :param internal_transactions:
        :return: None
        """
        uncles = body['fields']['Uncles'] or []
        transactions = body['fields']['Transactions'] or []
        block_number = header['block_number']
        block_hash = header['block_hash']

        block_reward, uncles_rewards = self.process_rewards(reward, block_number)
        block_data = self.process_header(header, block_reward)
        uncles_data = self.process_uncles(uncles, uncles_rewards, block_number, block_hash)
        transactions_data = self.process_transactions(transactions, block_number, block_hash)
        receipts_data, logs_data = self.process_receipts(receipts, transactions_data,
                                                         block_number, block_hash)
        accounts_data = self.process_accounts(accounts, block_number, block_hash)
        internal_txs_data = self.process_internal_txs(internal_transactions, block_number, block_hash)

        self.main_db.write_block_data(block_data, uncles_data, transactions_data, receipts_data,
                                      logs_data, accounts_data, internal_txs_data)

    def process_rewards(self, reward, block_number):
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

    def process_header(self, header, reward):
        data = dict_keys_case_convert(header['fields'])
        data.update(reward)
        hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
        return data

    def process_uncles(self, uncles, rewards, block_number, block_hash):
        items = []
        for i, uncle in enumerate(uncles):
            rwd = rewards[i]
            data = dict_keys_case_convert(uncle)
            assert rwd['UnclePosition'] == i
            data['reward'] = rwd['UncleReward']
            data['block_hash'] = block_hash
            data['block_number'] = block_number
            hex_vals_to_int(data, ['number', 'gas_used', 'gas_limit', 'timestamp', 'difficulty'])
            items.append(data)
        return items

    def process_transactions(self, transactions, block_number, block_hash):
        items = []
        for i, tx in enumerate(transactions):
            tx_data = dict_keys_case_convert(tx)
            tx_data['transaction_index'] = i
            tx_data['block_hash'] = block_hash
            tx_data['block_number'] = block_number
            if tx['to'] is None:
                tx_data['to'] = contracts.NULL_ADDRESS
            items.append(tx_data)
        return items

    def process_receipts(self, receipts, transactions, block_number, block_hash):
        rdata = receipts['fields']['Receipts'] or []
        recpt_items = []
        logs_items = []
        for i, receipt in enumerate(rdata):
            recpt_data = dict_keys_case_convert(receipt)
            tx = transactions[i]
            assert tx['hash'] == recpt_data['transaction_hash']
            recpt_data['transaction_index'] = i
            recpt_data['to'] = tx['to']
            recpt_data['from'] = tx['from']
            logs = recpt_data.pop('logs') or []
            recpt_data['block_hash'] = block_hash
            recpt_data['block_number'] = block_number
            hex_vals_to_int(recpt_data, ['cumulative_gas_used', 'gas_used', 'status'])
            recpt_items.append(recpt_data)

            logs = self.process_logs(logs)
            logs_items.extend(logs)
        return recpt_items, logs_items

    def process_logs(self, logs):
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
            items.append(data)
        return items

    def process_accounts(self, accounts, block_number, block_hash):
        items = []
        for account in accounts:
            data = dict_keys_case_convert(account['fields'])
            data['storage'] = None  # FIXME!!!
            data['address'] = account['address'].lower()
            data['block_number'] = block_number
            data['block_hash'] = block_hash
            items.append(data)
        return items

    def process_internal_txs(self, internal_txs, block_number, block_hash):
        items = []
        for i, tx in enumerate(internal_txs, 1):
            data = dict_keys_case_convert(tx['fields'])
            data['timestamp'] = data.pop('time_stamp')
            data['transaction_index'] = i
            del data['operation']
            data['op'] = tx['type']
            items.append(data)
        return items


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
