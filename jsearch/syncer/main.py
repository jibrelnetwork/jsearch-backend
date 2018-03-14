import asyncio
import logging

import configargparse
from dotenv import load_dotenv, find_dotenv

from .service import Service

load_dotenv(find_dotenv())


logger = logging.getLogger(__name__)


def run():
    p = configargparse.ArgParser(description='jSearch DBs sync service.',
                                 default_config_files=['/etc/jsearch-syncer.conf'])
    p.add('-c', '--config', required=False, is_config_file=True, help='config file path')

    p.add('--main-db', required=True, help='Main Database connection string', env_var='JSEARCH_MAIN_DB')
    p.add('--raw-db', required=True, help='Raw Database connection string', env_var='JSEARCH_RAW_DB')
    p.add('--log-level', help='log level', default='INFO')

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
