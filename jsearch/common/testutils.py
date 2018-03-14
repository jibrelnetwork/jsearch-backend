import yoyo
import os
import os.path


def setup_database():
    backend = yoyo.get_backend(os.environ['JSEARCH_MAIN_DB_TEST'])
    migrations = yoyo.read_migrations(os.path.join(os.path.dirname(__file__), 'migrations'))
    backend.apply_migrations(backend.to_apply(migrations))


def teardown_database():
    backend = yoyo.get_backend(os.environ['JSEARCH_MAIN_DB_TEST'])
    migrations = yoyo.read_migrations(os.path.join(os.path.dirname(__file__), 'migrations'))
    backend.rollback_migrations(backend.to_rollback(migrations))
