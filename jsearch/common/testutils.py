import os
import os.path

from .alembic_utils import upgrade, downgrade


def setup_database():
    connection_string = os.environ['JSEARCH_MAIN_DB_TEST']
    upgrade(connection_string, 'head')


def teardown_database():
    connection_string = os.environ['JSEARCH_MAIN_DB_TEST']
    downgrade(connection_string, 'base')
