from datetime import datetime

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


def json_dump(connection_string):
    import json
    from sqlalchemy import create_engine
    import decimal

    engine = create_engine(connection_string)
    conn = engine.connect()

    tables = conn.execute("""SELECT table_name FROM information_schema.tables
                        WHERE table_schema='public' AND table_type='BASE TABLE';""")

    class Ecnoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, decimal.Decimal):
                return int(obj)
            if isinstance(obj, datetime):
                return obj.isoformat()
            return super().default(obj)

    res = {}
    for t in tables:
        q = "SELECT * FROM {};".format(t['table_name'])
        rows = [dict(r) for r in conn.execute(q)]
        res[t['table_name']] = rows
    print(json.dumps(res, indent=4, cls=Ecnoder))

