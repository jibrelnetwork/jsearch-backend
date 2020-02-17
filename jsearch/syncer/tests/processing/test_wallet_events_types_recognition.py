import pytest
from typing import NamedTuple, Any, Dict

from jsearch.common.wallet_events import get_event_type, WalletEventType, CANCELLATION_ADDRESS
from jsearch.typing import Transaction


class TransactionCase(NamedTuple):
    data: Dict[str, Any]
    is_pending: bool
    to_contract: bool
    expected_type: str

    id: str

    def to_tuple(self):
        return self.data, self.is_pending, self.to_contract, self.expected_type


txs_cases = [
    # Target is a contract
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0xa9059cbb'
                     '0000000000000000000000000d0707963952f2fba59dd06f2b425ace40b492fe'
                     '00000000000000000000000000000000000000000000225a2fd53f4ef6d44800',
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.ERC20_TRANSFER,
        id='from_tx_erc20_transfer_method_transfer'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x23b872dd'
                     '00000000000000000000000077fae6ac54240057b443a4d007627710f4e9c12e'
                     '000000000000000000000000a49e44aa6f7c1752e95e6de0f2043df04bb3f632'
                     '00000000000000000000000000000000000000000000000000000001cfb778b8'
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.ERC20_TRANSFER,
        id='from_tx_erc20_transfer_method_transfer_from'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.CONTRACT_CALL,
        id='from_tx_contract_call'
    ),
    TransactionCase(
        data={
            'value': "0x100",
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_tx_ether_transfer_to_contract'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_tx_cancellation_to_contract'
    ),
    TransactionCase(
        data={
            'value': '0x100',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=False,
        to_contract=False,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_tx_ether_transfer_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_tx_cancellation_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x'
        },
        is_pending=False,
        to_contract=False,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_tx_cancellation_to_not_contract'
    ),

    # - pending transactions
    TransactionCase(
        data={
            'value': '256',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=False,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_pending_tx_ether_transfer_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_pending_tx_cancellation_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0x0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0xa9059cbb'
                     '0000000000000000000000000d0707963952f2fba59dd06f2b425ace40b492fe'
                     '00000000000000000000000000000000000000000000225a2fd53f4ef6d44800',
        },
        is_pending=False,
        to_contract=True,
        expected_type=WalletEventType.ERC20_TRANSFER,
        id='from_pending_tx_erc20_transfer_method_transfer'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x23b872dd'
                     '00000000000000000000000077fae6ac54240057b443a4d007627710f4e9c12e'
                     '000000000000000000000000a49e44aa6f7c1752e95e6de0f2043df04bb3f632'
                     '00000000000000000000000000000000000000000000000000000001cfb778b8'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.ERC20_TRANSFER,
        id='from_pending_tx_erc20_transfer_method_transfer_from'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.CONTRACT_CALL,
        id='from_pending_tx_contract_call'
    ),
    TransactionCase(
        data={
            'value': '3107550999999999999',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_pending_tx_ether_transfer_to_contract'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_pending_tx_contract_call_to_contract'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=True,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_pending_tx_cancellation_to_contract'
    ),

    # Target is not a contract
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0xa9059cbb'
                     '0000000000000000000000000d0707963952f2fba59dd06f2b425ace40b492fe'
                     '00000000000000000000000000000000000000000000225a2fd53f4ef6d44800',
        },
        is_pending=True,
        to_contract=False,
        expected_type=WalletEventType.ERC20_TRANSFER,
        id='from_pending_tx_erc20_transfer_method_transfer_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '3107550999999999999',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=False,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_pending_tx_ether_transfer_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': CANCELLATION_ADDRESS,
            'input': '0x1a412cd2'
        },
        is_pending=True,
        to_contract=False,
        expected_type=WalletEventType.TX_CANCELLATION,
        id='from_pending_tx_cancellation_to_not_contract'
    ),
    TransactionCase(
        data={
            'value': '0',
            'from': '0x355941cf7ac065310fd4023e1b913209f076a48a',
            'to': '0xa5fd1a791c4dfcaacc963d4f73c6ae5824149ea7',
            'input': '0x'
        },
        is_pending=True,
        to_contract=False,
        expected_type=WalletEventType.ETH_TRANSFER,
        id='from_pending_tx_cancellation_to_not_contract'
    ),
]


@pytest.mark.parametrize(
    "data,is_pending,to_contract,expected",
    argvalues=[x.to_tuple() for x in txs_cases],
    ids=[x.id for x in txs_cases],
)
def test_find_event_type(data: Transaction, is_pending: bool, to_contract: bool, expected: str) -> None:
    # when
    result = get_event_type(data, is_pending=is_pending, is_receiver_contract=to_contract)

    # then
    assert result == expected
