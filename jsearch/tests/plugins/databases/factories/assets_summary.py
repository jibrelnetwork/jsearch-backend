from random import randint

import factory
import pytest
from functools import partial

from jsearch.common.tables import assets_summary_t, assets_summary_pairs_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class AssetsSummaryModel(Base):
    __table__ = assets_summary_t
    __mapper_args__ = {
        'primary_key': [
            assets_summary_t.c.asset_address,
            assets_summary_t.c.block_hash,
            assets_summary_t.c.address
        ]
    }


class AssetsSummaryPairModel(Base):
    __table__ = assets_summary_pairs_t
    __mapper_args__ = {
        'primary_key': [
            assets_summary_pairs_t.c.asset_address,
            assets_summary_pairs_t.c.address
        ]
    }


class AssetsSummaryFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    asset_address = factory.LazyFunction(generate_address)

    value = factory.LazyFunction(partial(randint, 1, 10 ** 8))
    decimals = factory.LazyFunction(partial(randint, 1, 18))

    tx_number = factory.LazyFunction(partial(randint, 1, 10000))
    nonce = factory.Sequence(lambda n: n)

    block_number = factory.Sequence(lambda n: n)
    block_hash = factory.LazyFunction(generate_address)

    class Meta:
        model = AssetsSummaryModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'

    @classmethod
    def maybe_create_with_pair(cls, assets_summary_pair_factory: 'AssetsSummaryPairFactory', **kwargs):
        instance = cls.create(**kwargs)

        if instance.asset_address != '':
            # WTF: Ether assets pairs are not stored in `assets_summary_pairs`.
            assets_summary_pair_factory.create(address=instance.address, asset_address=instance.asset_address)

        return instance


class AssetsSummaryPairFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)
    asset_address = factory.LazyFunction(generate_address)

    class Meta:
        model = AssetsSummaryPairModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def assets_summary_factory():
    yield AssetsSummaryFactory
    AssetsSummaryFactory.reset_sequence()


@pytest.fixture()
def assets_summary_pair_factory():
    yield AssetsSummaryPairFactory
    AssetsSummaryPairFactory.reset_sequence()
