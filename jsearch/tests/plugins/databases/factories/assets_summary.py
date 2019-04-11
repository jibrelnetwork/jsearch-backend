from functools import partial
from random import randint

import factory
import pytest

from jsearch.common.tables import assets_summary_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class AssetsSummaryModel(Base):
    __table__ = assets_summary_t
    __mapper_args__ = {
        'primary_key': [
            assets_summary_t.c.asset_address,
            assets_summary_t.c.address
        ]
    }


class AssetsSummaryFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    asset_address = factory.LazyFunction(generate_address)

    value = factory.LazyFunction(partial(randint, 1, 10 ** 8))
    decimals = factory.LazyFunction(partial(randint, 1, 18))

    tx_number = factory.LazyFunction(partial(randint, 1, 10000))
    nonce = factory.Sequence(lambda n: n)

    class Meta:
        model = AssetsSummaryModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def assets_summary_factory():
    return AssetsSummaryFactory
