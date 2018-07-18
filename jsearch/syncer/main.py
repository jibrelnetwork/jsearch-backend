import asyncio
import logging
import os

import configargparse

from .service import Service

logger = logging.getLogger(__name__)


def run():
    p = configargparse.ArgParser(description='jSearch DBs sync service.',
                                 default_config_files=['/etc/jsearch-syncer.conf'])
    p.add('--log-level', help='log level', default=os.getenv('LOG_LEVEL', 'INFO'))

    options = p.parse_args()

    logging.basicConfig(
        format='%(asctime)-15s %(levelname)-8s %(name)s: %(message)s',
        level=options.log_level
    )

    service = Service(options)
    service.run()

    loop = asyncio.get_event_loop()
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        service.stop()
        loop.stop()
        loop.close()


if __name__ == '__main__':
    run()
