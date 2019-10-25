import click

from jsearch.api.app import run_api


@click.command()
@click.option('-p', '--port', envvar="PORT")
@click.pass_obj
def api(config: 'AppConfig', port: int):
    """
    Ethereum explorer API
    """
    run_api(port, config.log_level, config.no_json_formatter)
