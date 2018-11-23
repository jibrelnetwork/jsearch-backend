import asyncio
import os

import configargparse

from jsearch.common import logs
from .service import Service


def run():
    p = configargparse.ArgParser(description='jSearch DBs sync service.')
    p.add('--log-level', help='log level', default=os.getenv('LOG_LEVEL', 'INFO'))
    p.add('--sync-range', help='blocks range to sync', default=None)

    options = p.parse_args()

    logs.configure(options.log_level)
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
