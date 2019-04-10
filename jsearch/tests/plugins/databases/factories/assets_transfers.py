from random import randint

import factory
import pytest

from jsearch.common.tables import assets_transfers_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class AssetsTransfersModel(Base):
    __table__ = assets_transfers_t
    __mapper_args__ = {
        'primary_key': [
            assets_transfers_t.c.block_hash,
            assets_transfers_t.c.asset_address,
            assets_transfers_t.c.address,
        ]
    }


class AssetsTransfersFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    asset_address = factory.LazyFunction(generate_address)

    type = 'erc20-transfer'

    from_ = factory.LazyFunction(generate_address)
    to = factory.LazyFunction(generate_address)

    value = factory.LazyFunction(lambda: randint(0, 10 ** 18))
    is_forked = False

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    ordering = 0

    class Meta:
        model = AssetsTransfersModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'
        rename = {'from_': 'from'}


@pytest.fixture()
def assets_transfers_factory():
    return AssetsTransfersFactory
