import logging

import click

from jsearch import settings
from jsearch.common import worker
from jsearch.common.logs import configure, select_formatter_class
from jsearch.common.services import ApiService
from jsearch.common.stats import setup_monitor_metrics
from jsearch.monitor.api import make_app
from jsearch.monitor.db import MainDB
from jsearch.monitor.services.consistency import ConsistencyCollector
from jsearch.monitor.services.db import DBService
from jsearch.monitor.services.lag import LagCollector
from jsearch.structs import AppConfig

logger = logging.getLogger("syncer")


@click.command()
@click.option('-p', '--port', type=int, default=settings.SYNCER_API_PORT)
@click.pass_obj
def monitor(config: AppConfig, port: int) -> None:
    """
    Service to check lag from different sources.
    """
    setup_monitor_metrics()

    configure(config.log_level, select_formatter_class(config.no_json_formatter))

    main_db = MainDB(connection_string=settings.JSEARCH_MAIN_DB)

    db = DBService(main_db)

    api = ApiService(app_maker=make_app, port=port)

    consistency_collector = ConsistencyCollector(main_db=main_db)
    consistency_collector.add_dependency(api)
    consistency_collector.add_dependency(db)

    lag_collector = LagCollector(main_db)
    lag_collector.add_dependency(api)
    lag_collector.add_dependency(db)

    worker.Worker(lag_collector, consistency_collector).execute_from_commandline()
