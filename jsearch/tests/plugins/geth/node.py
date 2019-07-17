import json
import logging
import shutil
from itertools import count
from pathlib import Path
from random import randint
from subprocess import Popen, PIPE
from typing import List, Optional

import attr
import pytest

logger = logging.getLogger(__name__)

pytest_plugings = (
    'jsearch.tests.plugins.databases.raw_db',
)

NODES_DIRECTORY = Path('/tmp/nodes')

chain_id = randint(20000, 100000)

node_counter = count()
p2p_port_counter = count(start=30303)


def execute(cmd: List[str], cwd: Optional[Path] = None, log_filename: str = 'logs.txt', wait: bool = False) -> Popen:
    with open(str(cwd / log_filename), 'w') as logfile:
        process = Popen(
            args=cmd,
            stdin=PIPE,
            stdout=logfile,
            stderr=logfile,
            cwd=cwd
        )

    if wait:
        process.wait()

    if process.returncode and process.returncode != 0:
        raise RuntimeError('Process was exited with return code: %s', process.returncode)

    return process


class KeyStore:
    store = {
        "UTC--2018-11-13T10-36-26.549567000Z--cb00926485fd2b4cf334cc35ce6d35792558830b": {
            "address": "cb00926485fd2b4cf334cc35ce6d35792558830b",
            "crypto": {
                "cipher": "aes-128-ctr",
                "ciphertext": "6259ea32e6d65b872a35a9376a4083ce22f57e2543e2c665933ee80e97c8e96f",
                "cipherparams": {
                    "iv": "1197a48f81d8941c95084a8e7bfac3e1"
                },
                "kdf": "scrypt",
                "kdfparams": {
                    "dklen": 32,
                    "n": 262144, "p": 1,
                    "r": 8,
                    "salt": "0913d277fd8d619e2ae7e95561fcdeb1b677c73762c73ecc0fb825252c65132a"
                },
                "mac": "144f6e18b2596ee2c3002c4f68631709e29154e3969d0e27d3762b3018c2079e"
            },
            "id": "6bda7d68-d127-4397-91df-689ccf55c1da",
            "version": 3
        }
    }

    @property
    def addresses(self) -> List[str]:
        return [account['address'] for account in self.store.values()]

    @property
    def default(self):
        return f"0x{self.addresses[0]}"

    def save(self, directory: Path) -> None:
        keystore_path = directory / "keystore"
        keystore_path.mkdir(exist_ok=True)
        for filename, values in self.store.items():
            content = json.dumps(values)

            path = keystore_path / filename
            path.write_text(content)

            logger.info("Created new keystore", extra={'path': path})


@attr.s
class Genesis:
    accounts: List[str] = attr.ib(factory=list)

    @property
    def description(self):
        return {
            "config": {
                "chainId": chain_id,
                "homesteadBlock": 0,
                "eip155Block": 0,
                "eip158Block": 0,
                "byzantiumBlock": 0
            },
            "difficulty": "1",
            "gasLimit": "1000000",
            "alloc": {account: {"balance": "1000000000000000000000000000"} for account in self.accounts}
        }

    def save(self, directory: Path, name: str = "genesis.json") -> Path:
        content = json.dumps(self.description)

        path = directory / name
        path.write_text(data=content)

        return path


@attr.s
class Node:
    node_id: str = attr.ib(factory=lambda: str(next(node_counter)))

    rpc_api: str = "eth"
    rpc_addr: str = "localhost"
    rpc_port: str = "8575"
    rpc_corsdomain: str = "*"

    p2p_port: str = attr.ib(factory=lambda: str(next(p2p_port_counter)))

    node_process: Optional[Popen] = None
    keystore: Optional[KeyStore] = None
    genesis: Optional[Genesis] = None

    @property
    def root(self) -> Path:
        return NODES_DIRECTORY / str(chain_id)

    @property
    def data_dir(self) -> Path:
        return self.root / self.node_id / "Data"

    @property
    def url(self):
        return f"http://{self.rpc_addr}:{self.rpc_port}"

    def terminate(self):
        if self.node_process:
            self.node_process.kill()

    def clean(self):
        if self.data_dir.exists():
            logger.info('Cleaning data dir', extra={'path': self.root})
            shutil.rmtree(path=str(self.data_dir))

    def init_chain(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.keystore = KeyStore()
        self.keystore.save(directory=self.data_dir)

        self.genesis = Genesis(accounts=self.keystore.addresses)
        genesis_path = self.genesis.save(directory=self.root)

        cmd = ["geth", "--datadir", self.data_dir, "init", genesis_path]

        logger.info(
            "Initializing chain",
            extra={
                'chain_root': self.root,
                'init_cmd': " ".join(map(str, cmd)),
            },
        )

        execute(cmd=cmd, cwd=self.root, wait=True, log_filename=f"{chain_id}_init_{self.node_id}.txt")

    def run(self):
        cmd = self.get_cmd()

        logger.info(
            "Running.",
            extra={
                'geth_cmd': " ".join(map(str, cmd)),
                'working_directory': self.root,
            },
        )

        self.node_process = execute(cmd=cmd, cwd=self.root, log_filename=f"node_{self.node_id}_logs.txt")

    @classmethod
    def as_new_process(cls, *args, **kwargs):
        node: Node = cls(*args, **kwargs)
        node.clean()
        node.init_chain()
        node.run()

        return node


@attr.s
class GethForkNode(Node):
    extdb_dsn: str = attr.ib(default="")

    rpc_api: str = "eth,net,admin"
    rpc_port: str = "8580"

    def get_cmd(self) -> List[str]:
        return [
            "geth-fork",
            "--nodiscover",
            "--datadir", self.data_dir,
            "--networkid", str(chain_id),
            "--port", self.p2p_port,
            "--rpc",
            "--rpcapi", self.rpc_api,
            "--rpcaddr", self.rpc_addr,
            "--rpcport", self.rpc_port,
            "--syncmode", "full",
            "--cache", "4096",
            "--extdb", self.extdb_dsn
        ]


@attr.s
class GethNode(Node):
    rpc_api: str = "admin,miner,db,eth,net,web3,personal,debug"
    rpc_port: str = "8575"

    def get_cmd(self) -> List[str]:
        return [
            "geth",
            "--nodiscover",
            "--datadir", self.data_dir,
            "--networkid", str(chain_id),
            "--port", self.p2p_port,
            "--rpc",
            "--rpcapi", self.rpc_api,
            "--rpcaddr", self.rpc_addr,
            "--rpcport", self.rpc_port,
            "--syncmode", "full",
            "--cache", "4096",
        ]


@pytest.fixture(scope="session")
def geth_fork_node(raw_db_dsn):
    node = GethForkNode.as_new_process(extdb_dsn=raw_db_dsn)
    yield node
    node.terminate()


@pytest.fixture(scope="session")
def geth_node():
    node = GethNode.as_new_process()
    yield node
    node.terminate()


@pytest.fixture(scope="session")
def geth_fork_rpc(geth_fork_node: Node) -> str:
    return geth_fork_node.url


@pytest.fixture(scope="session")
def geth_rpc(geth_node: Node, ) -> str:
    return geth_node.url
