import factory
import pytest

from jsearch.common.tables import accounts_base_t
from jsearch.tests.plugins.databases.factories.common import generate_address
from .common import session, Base


class AccountBaseModel(Base):
    __table__ = accounts_base_t


class AccountFactory(factory.alchemy.SQLAlchemyModelFactory):
    address = factory.LazyFunction(generate_address)

    code = ""
    code_hash = ""

    root = ""

    last_known_balance = 0

    class Meta:
        model = AccountBaseModel
        sqlalchemy_session = session
        sqlalchemy_session_persistence = 'flush'


@pytest.fixture()
def account_factory():
    return AccountFactory
