import pytest
from sqlalchemy.engine import Connection
from typing import Any

from jsearch.common.tables import notable_accounts_t
from jsearch.notable_accounts_worker import services
from jsearch.notable_accounts_worker.services.notable_accounts import InvalidMessageFormat


@pytest.mark.parametrize(
    'message',
    [
        None,
        'string',
        123,
        {'dict': '.'},
        ['list', 'of', 'items'],
        [{'list': 'of'}, {'dicts': '.'}],
    ]
)
async def test_notable_accounts_service_complains_if_message_is_invalid(
        notable_accounts_service: services.NotableAccountsService,
        message: Any,
) -> None:
    with pytest.raises(InvalidMessageFormat):
        await notable_accounts_service.handle_notable_accounts(message)


async def test_notable_accounts_service_inserts_a_single_item_to_database(
        db: Connection,
        notable_accounts_service: services.NotableAccountsService,
) -> None:
    await notable_accounts_service.handle_notable_accounts(
        [
            {
                'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
                'name': '0x: Token Transfer Proxy',
                'labels': ['0x Ecosystem'],
            },
        ]
    )

    in_db = db.execute(notable_accounts_t.select()).fetchall()
    in_db = [dict(item) for item in in_db]

    assert in_db == [
        {
            'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
            'name': '0x: Token Transfer Proxy',
            'labels': ['0x Ecosystem'],
        },
    ]


async def test_notable_accounts_service_does_not_override_an_item_if_requested(
        db: Connection,
        notable_accounts_service: services.NotableAccountsService,
) -> None:
    notable_accounts_service.update_if_exists = False

    db.execute(notable_accounts_t.insert().values(
        {
            'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
            'name': '0x: Token Transfer Proxy',
            'labels': ['0x Ecosystem'],
        },
    ))

    await notable_accounts_service.handle_notable_accounts(
        [
            {
                'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
                'name': 'NEW NAME FOR A TOKEN',
                'labels': ['0x Ecosystem', 'ANOTHER LABEL'],
            },
        ]
    )

    in_db = db.execute(notable_accounts_t.select()).fetchall()
    in_db = [dict(item) for item in in_db]

    assert in_db == [
        {
            'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
            'name': '0x: Token Transfer Proxy',
            'labels': ['0x Ecosystem'],
        },
    ]


async def test_notable_accounts_service_overrides_an_item_if_requested(
        db: Connection,
        notable_accounts_service: services.NotableAccountsService,
) -> None:
    db.execute(notable_accounts_t.insert().values(
        {
            'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
            'name': '0x: Token Transfer Proxy',
            'labels': ['0x Ecosystem'],
        },
    ))

    await notable_accounts_service.handle_notable_accounts(
        [
            {
                'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
                'name': 'NEW NAME FOR A TOKEN',
                'labels': ['0x Ecosystem', 'ANOTHER LABEL'],
            },
        ]
    )

    in_db = db.execute(notable_accounts_t.select()).fetchall()
    in_db = [dict(item) for item in in_db]

    assert in_db == [
        {
            'address': '0x8da0d80f5007ef1e431dd2127178d224e32c2ef4',
            'name': 'NEW NAME FOR A TOKEN',
            'labels': ['0x Ecosystem', 'ANOTHER LABEL'],
        },
    ]
