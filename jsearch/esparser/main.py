import logging
import os

import configargparse

from jsearch.esparser.service import Service

logger = logging.getLogger(__name__)


def run():
    p = configargparse.ArgParser(description='jSearch Etherscan contracts parser.',
                                 default_config_files=['/etc/jsearch-separser.conf'])
    p.add('--log-level', help='log level', default=os.getenv('LOG_LEVEL', 'INFO'))

    options = p.parse_args()

    logging.basicConfig(
        format='%(asctime)-15s %(levelname)-8s %(name)s: %(message)s',
        level=options.log_level
    )

    service = Service(options)

    try:
        service.run()
    except KeyboardInterrupt:
        service.stop()


if __name__ == '__main__':
    run()
