from alembic.config import Config
from alembic import command
from os.path import abspath, dirname


def get_config(connection_string):
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", dirname(abspath(__file__)) + "/migrations")
    alembic_cfg.set_main_option("sqlalchemy.url", connection_string)
    alembic_cfg.config_file_name = dirname(dirname(dirname(abspath(__file__)))) + "/alembic.ini"

    return alembic_cfg

def upgrade(connection_string, revision):
    command.upgrade(get_config(connection_string), revision)


def revision(connection_string, message, autogenerate):
    command.revision(get_config(connection_string), message=message, autogenerate=autogenerate)


def downgrade(connection_string, revision):
    command.downgrade(get_config(connection_string), revision)


def init(path):
    command.init(get_config(""), path)
