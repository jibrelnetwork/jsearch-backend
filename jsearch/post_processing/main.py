# !/usr/bin/env python
import logging

import click

from jsearch.post_processing.service import service

logger = logging.getLogger(__name__)


@click.command()
@click.option('--log-level', default=logging.INFO, help="Log level")
def post_processing(log_level):
    logging.basicConfig(
        format='%(asctime)-15s %(levelname)-8s %(name)s: %(message)s',
        level=log_level
    )
    try:
        service.run()
    except Exception as e:
        logging.error(e)
    finally:
        service.stop()


def run():
    try:
        post_processing()
    except click.Abort:
        print('[FAIL] Abort')


if __name__ == '__main__':
    run()
