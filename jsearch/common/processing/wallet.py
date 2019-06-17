from jsearch.common.wallet_events import event_from_tx, event_from_token_transfer, event_from_internal_tx


def events_from_transactions(transactions, contracts_set):
    return [event_from_tx(tx['address'], tx, is_receiver_contract=tx['to'] in contracts_set) for tx in transactions]


def events_from_transfers(transfers, transactions):
    tx_map = {tx['hash']: tx for tx in transactions}
    events = []
    for t in transfers:
        e = event_from_token_transfer(t['address'], t, tx_map[t['transaction_hash']])
        events.append(e)
    return events


def events_from_internal_transactions(internal_transactions, transactions):
    tx_map = {tx['hash']: tx for tx in transactions}
    events = []
    for it in internal_transactions:
        events.append(event_from_internal_tx(it['from'], it, tx_map[it['parent_tx_hash']]))
        events.append(event_from_internal_tx(it['to'], it, tx_map[it['parent_tx_hash']]))
    return events


def assets_from_accounts(accounts):
    """
     address       | character varying |           | not null |
     asset_address | character varying |           | not null |
     tx_number     | integer           |           |          |
     nonce         | integer           |           |          |
     value         | numeric           |           |          |
     decimals      | integer           |           |          |
    :param accounts:
    :return:
    """
    updates = []
    for acc in accounts:
        update_data = {
            'address': acc['address'],
            'asset_address': '',
            'value': acc['balance'],
            'decimals': 0
        }
        updates.append(update_data)
    return updates


def assets_from_token_balance_updates(token_balance_updates):
    updates = []
    for balance in token_balance_updates:
        update_data = {
            'address': balance['account_address'],
            'asset_address': balance['token_address'],
            'value': balance['balance'],
            'decimals': balance['decimals']
        }
        updates.append(update_data)
    return updates
