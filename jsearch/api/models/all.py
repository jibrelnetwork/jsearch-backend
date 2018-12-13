from typing import List, Dict, Any

from jsearch.api.models.base_model_ import Model


class Log(Model):
    swagger_types = {
        'address': str,
        'block_hash': str,
        'block_number': int,
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


class Transaction(Model):
    swagger_types = {
        'block_hash': str,
        'block_number': int,
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
    }

    attribute_map = {
        'block_hash': 'blockHash',
        'block_number': 'blockNumber',
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
    }


class Block(Model):
    swagger_types = {
        'difficulty': int,
        'extra_data': str,
        'gas_limit': int,
        'gas_used': int,
        'hash': str,
        'logs_bloom': str,
        'miner': str,
        'mix_hash': str,
        'nonce': str,
        'number': int,
        'parent_hash': str,
        'receipts_root': str,
        'sha3_uncles': str,
        'size': int,
        'state_root': str,
        'timestamp': int,
        'total_difficulty': int,
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
        'size': 'size',
        'state_root': 'stateRoot',
        'timestamp': 'timestamp',
        'total_difficulty': 'totalDifficulty',
        'transactions': 'transactions',
        'transactions_root': 'transactionsRoot',
        'uncles': 'uncles',
        'static_reward': 'staticReward',
        'uncle_inclusion_reward': 'uncleInclusionReward',
        'tx_fees': 'txFees',
    }


class Uncle(Model):
    swagger_types = {
        'difficulty': int,
        'extra_data': str,
        'gas_limit': int,
        'gas_used': int,
        'hash': str,
        'logs_bloom': str,
        'miner': str,
        'mix_hash': str,
        'nonce': str,
        'number': int,
        'parent_hash': str,
        'receipts_root': str,
        'sha3_uncles': str,
        'size': int,
        'state_root': str,
        'timestamp': int,
        'total_difficulty': int,
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
        'size': 'size',
        'state_root': 'stateRoot',
        'timestamp': 'timestamp',
        'total_difficulty': 'totalDifficulty',
        'transactions_root': 'transactionsRoot',
        'block_number': 'blockNumber',
        'reward': 'reward',
    }


class Receipt(Model):
    swagger_types = {
        'block_hash': str,
        'block_number': int,
        'contract_address': str,
        'cumulative_gas_used': int,
        'from': str,
        'gas_used': int,
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


class Reward(Model):
    swagger_types = {
        'address': str,
        'amount': int,
    }

    attribute_map = {
        'address': 'address',
        'amount': 'amount',
    }


class Web3Call(Model):
    swagger_types = {
        'method': str,
        'arguments': List[Any],
    }

    attribute_map = {
        'method': 'method',
        'arguments': 'arguments',
    }


class Web3CallResponse(Model):
    swagger_types = {
        'status': str,
        'result': Any,
    }

    attribute_map = {
        'status': 'status',
        'result': 'result',
    }


class Balance(Model):
    swagger_types = {
        'balance': int,
        'address': str,
    }

    attribute_map = {
        'balance': 'balance',
        'address': 'address',
    }


class TokenTransfer(Model):
    swagger_types = {
        'transaction': str,
        'from': str,
        'block_hash': str,
        'to': str,
        'amount': int,
    }

    attribute_map = {
        'transaction': 'transaction',
        'from': 'from',
        'to': 'to',
        'amount': 'amount',
        'block_hash': 'blockHash'
    }

    @classmethod
    def from_log_record(cls, log):
        data: Dict[str, Any] = {
            'transaction': log['transaction_hash'],
            'from': log['token_transfer_from'],
            'to': log['token_transfer_to'],
            'amount': log['token_amount'],
            'block_hash': log['block_hash']
        }
        return cls(**data)
