import json

from typing import List, Dict, Any

from jsearch.api.models.base_model_ import Model


class Log(Model):
    swagger_types = {
        'address': str,
        'block_hash': str,
        'block_number': int,
        'timestamp': int,
        'data': str,
        'log_index': int,
        'removed': str,
        'topics': List[str],
        'transaction_hash': str,
        'transaction_index': int,
    }

    attribute_map = {
        'address': 'address',
        'block_hash': 'blockHash',
        'block_number': 'blockNumber',
        'timestamp': 'timestamp',
        'data': 'data',
        'log_index': 'logIndex',
        'removed': 'removed',
        'topics': 'topics',
        'transaction_hash': 'transactionHash',
        'transaction_index': 'transactionIndex',
    }


class Account(Model):
    swagger_types = {
        'block_number': int,
        'block_hash': str,
        'address': str,
        'nonce': int,
        'code': str,
        'code_hash': str,
        'balance': int,
    }

    attribute_map = {
        'block_number': 'blockNumber',
        'block_hash': 'blockHash',
        'address': 'address',
        'nonce': 'nonce',
        'code': 'code',
        'code_hash': 'codeHash',
        'balance': 'balance',
    }

    int_to_str = {'balance'}


class Transaction(Model):
    swagger_types = {
        'block_hash': str,
        'block_number': int,
        'timestamp': int,
        'from': str,
        'gas': str,
        'gas_price': str,
        'hash': str,
        'input': str,
        'nonce': str,
        'r': str,
        's': str,
        'to': str,
        'transaction_index': int,
        'v': str,
        'value': str,
        'status': bool
    }

    attribute_map = {
        'block_hash': 'blockHash',
        'block_number': 'blockNumber',
        'timestamp': 'timestamp',
        'from': 'from',
        'gas': 'gas',
        'gas_price': 'gasPrice',
        'hash': 'hash',
        'input': 'input',
        'nonce': 'nonce',
        'r': 'r',
        's': 's',
        'to': 'to',
        'transaction_index': 'transactionIndex',
        'v': 'v',
        'value': 'value',
        'status': 'status'
    }

    def to_dict(self):
        data = super(Transaction, self).to_dict()
        data['status'] = bool(data['status'])
        return data


class InternalTransaction(Model):
    swagger_types = {
        'block_number': int,
        'block_hash': str,
        'timestamp': int,
        'parent_tx_hash': str,
        'parent_tx_index': int,
        'op': str,
        'call_depth': int,
        'from': str,
        'to': str,
        'value': str,
        'gas_limit': str,
        'payload': str,
        'status': str,
        'transaction_index': int,
    }

    attribute_map = {
        'block_number': 'blockNumber',
        'block_hash': 'blockHash',
        'timestamp': 'timestamp',
        'parent_tx_hash': 'parentTxHash',
        'parent_tx_index': 'parentTxIndex',
        'op': 'op',
        'call_depth': 'callDepth',
        'from': 'from',
        'to': 'to',
        'value': 'value',
        'gas_limit': 'gasLimit',
        'payload': 'input',
        'status': 'status',
        'transaction_index': 'transactionIndex',
    }

    int_to_str = {'value', 'gasLimit'}


class PendingTransaction(Model):
    swagger_types = {
        'hash': str,
        'status': str,
        'removed': str,
        'r': str,
        's': str,
        'v': str,
        'to': str,
        'from': str,
        'gas': str,
        'gas_price': str,
        'input': str,
        'nonce': str,
        'value': str,
    }

    attribute_map = {
        'hash': 'hash',
        'status': 'status',
        'removed': 'removed',
        'r': 'r',
        's': 's',
        'v': 'v',
        'to': 'to',
        'from': 'from',
        'gas': 'gas',
        'gas_price': 'gasPrice',
        'input': 'input',
        'nonce': 'nonce',
        'value': 'value',
    }

    int_to_str = {'gas', 'gasPrice', 'nonce', 'value'}


class Block(Model):
    swagger_types = {
        'difficulty': str,
        'extra_data': str,
        'gas_limit': str,
        'gas_used': str,
        'hash': str,
        'logs_bloom': str,
        'miner': str,
        'mix_hash': str,
        'nonce': str,
        'number': int,
        'parent_hash': str,
        'receipts_root': str,
        'sha3_uncles': str,
        'state_root': str,
        'timestamp': int,
        'transactions': List[str],
        'transactions_root': str,
        'uncles': List[str],
        'static_reward': int,
        'uncle_inclusion_reward': int,
        'tx_fees': int,
    }

    attribute_map = {
        'difficulty': 'difficulty',
        'extra_data': 'extraData',
        'gas_limit': 'gasLimit',
        'gas_used': 'gasUsed',
        'hash': 'hash',
        'logs_bloom': 'logsBloom',
        'miner': 'miner',
        'mix_hash': 'mixHash',
        'nonce': 'nonce',
        'number': 'number',
        'parent_hash': 'parentHash',
        'receipts_root': 'receiptsRoot',
        'sha3_uncles': 'sha3Uncles',
        'state_root': 'stateRoot',
        'timestamp': 'timestamp',
        'transactions': 'transactions',
        'transactions_root': 'transactionsRoot',
        'uncles': 'uncles',
        'static_reward': 'staticReward',
        'uncle_inclusion_reward': 'uncleInclusionReward',
        'tx_fees': 'txFees',
    }

    int_to_str = {'staticReward', 'uncleInclusionReward', 'txFees',
                  'difficulty', 'gasLimit', 'gasUsed'}


class Uncle(Model):
    swagger_types = {
        'difficulty': str,
        'extra_data': str,
        'gas_limit': str,
        'gas_used': str,
        'hash': str,
        'logs_bloom': str,
        'miner': str,
        'mix_hash': str,
        'nonce': str,
        'number': int,
        'parent_hash': str,
        'receipts_root': str,
        'sha3_uncles': str,
        'state_root': str,
        'timestamp': int,
        'transactions_root': str,
        'block_number': int,
        'reward': int,
    }

    attribute_map = {
        'difficulty': 'difficulty',
        'extra_data': 'extraData',
        'gas_limit': 'gasLimit',
        'gas_used': 'gasUsed',
        'hash': 'hash',
        'logs_bloom': 'logsBloom',
        'miner': 'miner',
        'mix_hash': 'mixHash',
        'nonce': 'nonce',
        'number': 'number',
        'parent_hash': 'parentHash',
        'receipts_root': 'receiptsRoot',
        'sha3_uncles': 'sha3Uncles',
        'state_root': 'stateRoot',
        'timestamp': 'timestamp',
        'transactions_root': 'transactionsRoot',
        'block_number': 'blockNumber',
        'reward': 'reward',
    }

    int_to_str = {'reward', 'difficulty', 'gasLimit', 'gasUsed'}


class Receipt(Model):
    swagger_types = {
        'block_hash': str,
        'block_number': int,
        'contract_address': str,
        'cumulative_gas_used': str,
        'from': str,
        'gas_used': str,
        'logs': List[Log],
        'logs_bloom': str,
        'root': str,
        'to': str,
        'transaction_hash': str,
        'transaction_index': int,
        'status': int,
    }

    attribute_map = {
        'block_hash': 'blockHash',
        'block_number': 'blockNumber',
        'contract_address': 'contractAddress',
        'cumulative_gas_used': 'cumulativeGasUsed',
        'from': 'from',
        'gas_used': 'gasUsed',
        'logs': 'logs',
        'logs_bloom': 'logsBloom',
        'root': 'root',
        'to': 'to',
        'transaction_hash': 'transactionHash',
        'transaction_index': 'transactionIndex',
        'status': 'status',
    }
    int_to_str = {'cumulativeGasUsed', 'gasUsed'}


class Reward(Model):
    swagger_types = {
        'address': str,
        'amount': int,
    }

    attribute_map = {
        'address': 'address',
        'amount': 'amount',
    }

    int_to_str = {'amount'}


class Balance(Model):
    swagger_types = {
        'balance': int,
        'address': str,
    }

    attribute_map = {
        'balance': 'balance',
        'address': 'address',
    }

    int_to_str = {'balance'}


class TokenTransfer(Model):
    swagger_types = {
        "transaction_hash": str,
        "timestamp": int,
        "from_address": str,
        "to_address": str,
        "token_address": str,
        "token_value": int,
        "token_decimals": int,
        "block_number": str,
        "log_index": str,
        "transaction_index": str,
    }

    attribute_map = {
        "transaction_hash": "transactionHash",
        "timestamp": "timestamp",
        "block_number": "blockNumber",
        "transaction_index": "transactionIndex",
        "log_index": "logIndex",
        "from_address": "from",
        "to_address": "to",
        "token_address": "contractAddress",
        "token_value": "amount",
        "token_decimals": "decimals",
    }

    int_to_str = {"amount"}


class TokenHolder(Model):
    swagger_types = {
        'account_address': str,
        'token_address': str,
        'balance': float,
        'decimals': int
    }

    attribute_map = {
        'account_address': 'accountAddress',
        'token_address': 'contractAddress',
        'balance': 'balance',
        'decimals': 'decimals'
    }


class TokenHolderWithId(Model):
    swagger_types = {
        'id': int,
        'account_address': str,
        'token_address': str,
        'balance': float,
        'decimals': int
    }

    attribute_map = {
        'id': 'id',
        'account_address': 'accountAddress',
        'token_address': 'contractAddress',
        'balance': 'balance',
        'decimals': 'decimals'
    }


class AssetTransfer(Model):
    swagger_types = {
        'type': str,
        'from': str,
        'to': str,
        'asset_address': str,
        'amount': float,
        'tx_data': Transaction
    }
    attribute_map = {
        'type': 'type',
        'from': 'from',
        'to': 'to',
        'asset_address': 'assetAddress',
        'amount': 'amount',
        'tx_data': 'txData',
    }


class WalletEvent(Model):
    swagger_types = {
        "type": str,
        "event_index": int,
        "event_data": Dict[str, Any],
    }
    attribute_map = {
        'type': 'eventType',
        'event_index': 'eventIndex',
        'event_data': 'eventData'
    }

    def to_dict(self):
        data = super(WalletEvent, self).to_dict()

        event_data = getattr(self, 'event_data', {})
        if isinstance(event_data, str):
            event_data = json.loads(event_data)

        data['eventData'] = [{'fieldName': name, 'fieldValue': value} for name, value in event_data.items()]

        return data


class EthTransfer(Model):
    swagger_types = {
        'timestamp': int,
        'tx_hash': str,
        'to': str,
        'from': str,
        'amount': str,
        'block_number': int,
        'event_index': int,
    }
    attribute_map = {
        'tx_hash': 'transactionHash',
        'timestamp': 'timestamp',
        'to': 'to',
        'from': 'from',
        'amount': 'amount',
        'block_number': 'block_number',
        'event_index': 'event_index',
    }
