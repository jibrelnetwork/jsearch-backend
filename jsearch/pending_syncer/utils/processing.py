from typing import List, Dict, Any, Optional

from jsearch.pending_syncer.structs import PendingTransaction


def prepare_pending_txs(txs_raw: List[Dict[str, Any]]) -> List[PendingTransaction]:
    """Remaps and dedupes a batch of RawDB's TXs into unique batch for MainDB.

    This function processes a batch of RawDB's TXs for making a bulk `UPSERT`.
    Without deduplication, PostgreSQL won't accept statements affecting the same
    row.
    """
    txs: Dict[str, PendingTransaction] = {}

    for tx_raw in sorted(txs_raw, key=lambda x: x['id']):
        tx_hash = tx_raw['tx_hash']
        tx = txs.get(tx_hash)
        txs[tx_hash] = maybe_update_pending_tx(tx, tx_raw)

    return list(txs.values())


def maybe_update_pending_tx(tx: Optional[PendingTransaction], newer_tx_raw: Dict[str, Any]) -> PendingTransaction:
    if tx is None:
        return PendingTransaction.from_raw_tx(newer_tx_raw)

    return update_from_newer_tx_raw(tx, newer_tx_raw)


def update_from_newer_tx_raw(tx: PendingTransaction, newer_tx_raw: Dict[str, Any]) -> PendingTransaction:
    # WTF: `newer_tx_raw` is the TX with a bigger ID.
    return PendingTransaction(
        hash=tx.hash,
        last_synced_id=newer_tx_raw['id'],
        status=newer_tx_raw['status'],
        timestamp=newer_tx_raw['timestamp'],
        removed=newer_tx_raw['removed'],
        node_id=newer_tx_raw['node_id'],
        # WTF: Populate additional TX's fields if previous TX didn't have them.
        # This can happen, if the first TX in a batch didn't have the fields
        # (this can happen, when TX is marked in the `pending_transactions` as
        # `removed=True`) but the second one did (i.e. `removed=False`).
        r=tx.r or newer_tx_raw['fields'].get('r'),
        s=tx.s or newer_tx_raw['fields'].get('s'),
        v=tx.v or newer_tx_raw['fields'].get('v'),
        to=tx.to or newer_tx_raw['fields'].get('to'),
        from_=tx.from_ or newer_tx_raw['fields'].get('from'),
        gas=tx.gas or newer_tx_raw['fields'].get('gas'),
        gas_price=tx.gas_price or newer_tx_raw['fields'].get('gasPrice'),
        input=tx.input or newer_tx_raw['fields'].get('input'),
        nonce=tx.nonce or newer_tx_raw['fields'].get('nonce'),
        value=tx.value or newer_tx_raw['fields'].get('value'),
    )
