import logging
import time

import pytest
from requests.exceptions import ConnectionError
from web3 import Web3

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

pytest_plugins = (
    "jsearch.tests.plugins.geth.node",
    "jsearch.tests.plugins.databases.raw_db"
)


class Web3ClientWrapper:
    url: str = "http://localhost:8575"

    miner_threads: int = 1
    wait_interval: int = 0.1  # seconds

    def __init__(self, url):
        self.client: Web3 = Web3(Web3.HTTPProvider(url))

    @property
    def last_block_number(self):
        return self.client.eth.getBlock('latest')['number']

    def start_miner(self):
        self.client.miner.start(self.miner_threads)

    def stop_miner(self):
        self.client.miner.stop()

    def mine_block(self):
        block_on_start = self.last_block_number

        logger.info('Start mining...', extra={'last_block_number': block_on_start})

        self.start_miner()
        start_time = time.time()
        try:
            while block_on_start == self.last_block_number:
                time.sleep(self.wait_interval)
                logger.info('Still mining...', extra={'time_elapsed': time.time() - start_time})
        except Exception as e:
            print(e)

        logger.info('Stop mining')
        self.stop_miner()

    def wait_node(self, attempts=5, waiting_interval=2):
        for i in range(0, attempts):
            try:
                logger.info('Waiting node...', extra={'last_block_number': self.last_block_number})
                return
            except ConnectionError:
                logger.info("Still waiting node...")
                time.sleep(waiting_interval)


@pytest.fixture(scope="session")
def w3(geth_rpc: str, geth_fork_rpc: str):
    client = Web3ClientWrapper(url=geth_rpc)
    client.wait_node()

    logger.info("[Web3] Geth node is up")

    fork_client = Web3ClientWrapper(url=geth_fork_rpc)
    fork_client.wait_node()

    logger.info("[Web3] Geth fork node is up")

    enode = client.client.admin.nodeInfo['enode']
    fork_client.client.admin.addPeer(enode)

    logger.info("[Web3] Nodes are linked")

    return client
