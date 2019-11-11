from typing import Tuple, Set

from jsearch.typing import Accounts


def accounts_to_state_and_base_data(accounts: Accounts) -> Tuple[Accounts, Accounts]:
    accounts_state_data = []
    accounts_base_data = []
    address_set: Set[str] = set()

    for account in accounts:
        if account['address'] not in address_set:
            address_set.add(account['address'])
            accounts_base_data.append({
                'address': account['address'],
                'code': account.get('code', ''),
                'code_hash': account['code_hash'],
                'last_known_balance': account['balance'],
                'root': account['root'],
                'is_forked': False,
            })

        accounts_state_data.append({
            'block_number': account['block_number'],
            'block_hash': account['block_hash'],
            'address': account['address'],
            'nonce': account['nonce'],
            'root': account['root'],
            'balance': account['balance'],
            'is_forked': False,
        })

    return accounts_state_data, accounts_base_data
