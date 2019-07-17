import factory
import pytest

from jsearch.common.tables import notable_accounts_t
from jsearch.tests.plugins.databases.factories import common


class NotableAccountModel(common.Base):
    __table__ = notable_accounts_t


class NotableAccountFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(common.generate_address)
    name = factory.Faker('company', locale='en_US')
    labels = factory.List(['Here', 'Goes', 'Labels'])

    class Meta:
        model = NotableAccountModel
        sqlalchemy_session = common.session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def notable_account_factory():
    return NotableAccountFactory
