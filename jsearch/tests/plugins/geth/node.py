import json
import logging
import shutil
from pathlib import Path
from subprocess import Popen, PIPE
from typing import List, Optional

import attr
import pytest

log = logging.getLogger(__name__)

pytest_plugings = (
    'jsearch.tests.plugins.databases.raw_db',
)

NODES_DIRECTORY = Path('/tmp/nodes')


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

            logging.info("Create new keystore %s", path)


@attr.s
class Genesis:
    chain_id: int = attr.ib(default=1)
    accounts: List[str] = attr.ib(factory=list)

    @property
    def description(self):
        return {
            "config": {
                "chainId": self.chain_id,
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
    chain_id: int = attr.ib(default=1)
    node_id: int = attr.ib(default=1)

    node_process: Optional[Popen] = None
    keystore: Optional[KeyStore] = None
    genesis: Optional[Genesis] = None

    rpc_api: str = "admin,miner,db,eth,net,web3,personal,debug"
    rpc_addr: str = "localhost"
    rpc_port: str = "8575"
    rpc_corsdomain: str = "*"

    @property
    def root(self) -> Path:
        return NODES_DIRECTORY / str(self.chain_id)

    @property
    def data_dir(self) -> Path:
        node_data = self.root / "Data"
        return node_data

    @property
    def url(self):
        return f"http://{self.rpc_addr}:{self.rpc_port}"

    def clean(self):
        if self.root.exists():
            logging.info('Clean %s', self.root)
            shutil.rmtree(path=str(self.root))

    def terminate(self):
        if self.node_process:
            self.node_process.kill()

    def init_chain(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.keystore = KeyStore()
        self.keystore.save(directory=self.data_dir)

        self.genesis = Genesis(chain_id=self.chain_id, accounts=self.keystore.addresses)
        genesis_path = self.genesis.save(directory=self.root)

        cmd = ["geth", "--datadir", self.data_dir, "init", genesis_path]

        logging.info("Chain root: %s", self.root)
        logging.info("Command to init chain: %s", " ".join(map(str, cmd)))

        execute(cmd=cmd, cwd=self.root, wait=True)

    def run(self, connection_string):
        cmd = [
            "geth",
            "--datadir", self.data_dir,
            "--networkid", str(self.chain_id),
            "--rpc",
            "--rpcapi", self.rpc_api,
            "--rpcaddr", self.rpc_addr,
            "--rpcport", self.rpc_port,
            "--syncmode", "full",
            "--cache", "4096",
            "--extdb", connection_string,
        ]

        logging.info("Geth command: %s", " ".join(map(str, cmd)))
        logging.info("Current working directory: %s", self.root)

        self.node_process = execute(cmd=cmd, cwd=self.root)

    @classmethod
    def as_new_process(cls, connection_string):
        node: Node = cls()
        node.clean()
        node.init_chain()
        node.run(connection_string=connection_string)

        return node


@pytest.fixture(scope="session")
def geth_node(raw_db_connection_string: str):
    node = Node.as_new_process(connection_string=raw_db_connection_string)
    yield node
    node.terminate()


@pytest.fixture(scope="session")
def geth_rpc(geth_node: Node, ) -> str:
    return geth_node.url
