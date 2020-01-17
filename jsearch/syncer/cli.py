import logging

import click

from jsearch import settings
from jsearch.common import worker
from jsearch.common.logs import configure, select_formatter_class
from jsearch.common.services import ApiService
from jsearch.common.stats import setup_monitor_metrics
from jsearch.structs import AppConfig
from .services.lag import make_app as lags_metrics_app, LagCollector

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
    api = ApiService(app_maker=lags_metrics_app, port=port)

    lag_collector = LagCollector()
    lag_collector.add_dependency(api)

    worker.Worker(lag_collector).execute_from_commandline()
