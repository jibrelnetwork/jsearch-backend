import asyncio
import json

import attr
import logging
from asyncio import Task
from decimal import Decimal

import aiohttp
import aiopg
import backoff
import itertools
import mode
import re
from aiopg.sa import Engine
from dateutil import parser
from lxml import html
from psycopg2.extras import DictCursor
from typing import NamedTuple, Dict, Any, List, Optional
from yarl import URL

from jsearch.data_checker import settings
from jsearch.common.db import fetch_all, fetch_one

logger = logging.getLogger(__name__)


class Transfer(NamedTuple):
    from_address: str
    to_address: str
    token_address: str
    amount: Decimal
    transaction_hash: str


FETCH_SLEEP_TIME = 1
REORG_WAIT_TIME = 5
ES_SCAN_DEPTH = 30


@backoff.on_exception(backoff.fibo, max_tries=5, exception=aiohttp.ClientError)
async def perform_request(url, **kwargs) -> str:
    def mask_password(url_: URL) -> URL:
        if url_.password is not None:
            return url_.with_password('********')

        return url_

    logger.info("Requesting %s", url)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, **kwargs) as resp:
                return await resp.text()
    except aiohttp.ClientResponseError as exc:
        masked_url = mask_password(exc.request_info.url)
        masked_real_url = mask_password(exc.request_info.real_url)

        exc.request_info = attr.evolve(exc.request_info, url=masked_url, real_url=masked_real_url)

        raise exc


class DataChecker(mode.Service):
    """
    Checking ERC20 tokens transfers by comparing
    with Etherscan transfers list https://etherscan.io/tokentxns
    """
    engine: Optional[Engine] = None

    def __init__(self, main_db_dsn: str, use_proxy: bool, proxy_load_url: Optional[str] = None, **kwargs) -> None:
        self.main_db_dsn = main_db_dsn
        self.total = 0
        self.check_queue: 'asyncio.Queue[Dict[str, Any]]' = asyncio.Queue()
        self.workers: List[Task] = []
        self.use_proxy = use_proxy
        self.proxy_load_url = proxy_load_url
        self.proxy_list: List[str] = []
        self.proxy_cycle = itertools.cycle(self.proxy_list)

        if self.use_proxy and not self.proxy_load_url:
            raise ValueError("'use_proxy' setting requires 'proxy_load_url'")

        super().__init__(**kwargs)

    async def on_start(self) -> None:
        await self.load_proxies()
        await self.connect()

    async def on_stop(self) -> None:
        await self.disconnect()

    async def connect(self) -> None:
        self.engine: Engine = await aiopg.sa.create_engine(
            self.main_db_dsn,
            cursor_factory=DictCursor,
            maxsize=settings.WORKERS
        )

    async def disconnect(self) -> None:
        if self.engine is None:
            return

        self.engine.close()
        await self.engine.wait_closed()

    async def worker(self, number):
        logger.info('Worker %s started', number)
        while not self.should_stop:
            block_to_check = await self.check_queue.get()
            try:
                await self.check_block(block_to_check)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception('Error when checking block', extra=block_to_check)
        logger.info('Worker %s is stopped', number)

    @mode.Service.task
    async def main_loop(self):
        logger.info('Enter main loop')
        last_fetched_block_number = 0
        for n in range(settings.WORKERS):
            self.workers.append(asyncio.create_task(self.worker(n)))
        while not self.should_stop:
            last_synced_block = await self.get_last_synced_block()
            if last_synced_block['number'] > last_fetched_block_number:
                logger.info('Block check enqueued, queue size: %s', self.check_queue.qsize(), extra=last_synced_block)
                await self.check_queue.put(last_synced_block)
                last_fetched_block_number = last_synced_block['number']
            await asyncio.sleep(FETCH_SLEEP_TIME)
        logger.info('Leaving main loop')

    async def load_proxies(self) -> None:
        if not self.use_proxy:
            return

        logger.info("Loading proxies")

        text = await perform_request(self.proxy_load_url)
        self.proxy_list = text.split()
        self.proxy_cycle = itertools.cycle(self.proxy_list)

        logger.info("Loaded %s proxies", len(self.proxy_list))

    def get_proxy(self):
        if self.use_proxy:
            host = next(self.proxy_cycle)
            return f'http://{settings.PROXY_USER}:{settings.PROXY_PASS}@{host}'

    async def get_page(self, url):
        proxy = self.get_proxy()
        text = await perform_request(url, proxy=proxy)
        tree = html.fromstring(text)
        return tree

    async def get_api_response(self, url):
        proxy = self.get_proxy()
        text = await perform_request(url, proxy=proxy)
        data = json.loads(text)
        result = data['result']
        return result

    async def es_get_block_info(self, block_number):
        hex_num = hex(block_number)
        url = f'https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag={hex_num}&boolean=true'
        block_data = await self.get_api_response(url)
        return block_data

    async def es_get_transfers(self, block_timestamp):
        transfers = []
        for page in range(2, ES_SCAN_DEPTH):
            et = await self.get_page(f'https://etherscan.io/tokentxns/?ps=100&p={page}')
            rows = et.xpath('//table//tr')
            for row in rows[1:]:
                transfer = {}
                row_data = [item.strip() for item in row.xpath('td//text()') if item.strip()]
                row_urls = row.xpath('td//a/@href')
                transfer['transaction_hash'] = self.get_address_from_url(row_urls[0])

                transfer['from_address'] = self.get_address_from_url(row_urls[1])
                transfer['to_address'] = self.get_address_from_url(row_urls[2])

                try:
                    transfer['amount'] = Decimal(row_data[5].replace(',', ''))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning('Transfer decimal parsing error: %s, %s, %s', row_data[5], block_timestamp, transfer)
                    continue
                transfer['token_address'] = self.get_address_from_url(row_urls[3])

                transfer_timestamp = self.parse_transfer_timestamp(row_data[1])
                if transfer_timestamp == block_timestamp:
                    if page == 1:
                        return await self.es_get_transfers(block_timestamp)
                    transfers.append(Transfer(**transfer))
                elif transfer_timestamp < block_timestamp:
                    return transfers
        return transfers

    async def check_block(self, block):
        logger.info('Checking block', extra=block)
        es_block_data = await self.es_get_block_info(block['number'])
        attempt = 1
        while block['hash'] != es_block_data['hash']:
            logger.info('Block hash mismatch, wait reorg', extra={
                'hash': block['hash'],
                'number': block['number'],
                'es_hash': es_block_data['hash'],
                'attempt': attempt,
            })
            await asyncio.sleep(REORG_WAIT_TIME)
            block = await self.get_synced_block_by_number(block['number'])
            es_block_data = await self.es_get_block_info(block['number'])
            attempt += 1

        synced_transfers = await self.get_synced_block_transfers(block['hash'])
        es_transfers = await self.es_get_transfers(int(es_block_data['timestamp'], 16))

        if not es_transfers:
            logger.info('Check complete: no transfers', extra=block)
            return

        synced_transfers_set = set(synced_transfers or [])
        es_transfers_set = set(es_transfers)

        logger.info('Transfers count ES/SYNC: %s/%s', len(es_transfers), len(synced_transfers), extra=block)

        for t in es_transfers_set:
            if t not in synced_transfers_set:
                logger.info('Try 18 decimals for %s', t)
                values = t._asdict()
                values['amount'] = t.amount / 10 ** 18
                tm = Transfer(**values)
                if tm not in synced_transfers_set:
                    logger.error('MISS FROM SYNCED %s', t, extra=block)
        logger.info('Check complete', extra=block)

    def parse_block_timestamp(self, time_stamp):
        res = re.findall(r'\((.*)\s\+UTC\)', time_stamp)
        if not res:
            return None
        try:
            dt = parser.parse(res[0])
        except ValueError:
            return None
        return dt.timestamp()

    def parse_transfer_timestamp(self, time_stamp):
        return parser.parse(time_stamp).timestamp()

    async def get_last_synced_block(self):
        """
        Not truly last synced,  but last - 6 blocks.
        6 blocks gap need to reduce influence of reorgs
        """
        q = """
            SELECT hash, number
            FROM blocks
            WHERE is_forked=false
            ORDER BY number DESC
            LIMIT 6;
        """
        rows = await fetch_all(self.engine, q)
        return rows[-1]

    async def get_synced_block_by_number(self, block_number):
        q = """
            SELECT hash, number
            FROM blocks
            WHERE number=%s
                AND is_forked=false;
        """
        return await fetch_one(self.engine, q, [block_number])

    async def get_synced_block_transfers(self, block_hash):
        q = """
            SELECT from_address, to_address, token_address, token_value, token_decimals, transaction_hash
            FROM token_transfers
            WHERE block_hash=%s;
        """
        rows = await fetch_all(self.engine, q, [block_hash])
        transfers = []
        for row in rows:
            transfer = Transfer(
                from_address=row['from_address'],
                to_address=row['to_address'],
                token_address=row['token_address'],
                amount=Decimal(row['token_value']) / 10 ** row['token_decimals'],
                transaction_hash=row['transaction_hash'],
            )
            transfers.append(transfer)
        return list(set(transfers))

    def get_address_from_url(self, url):
        matches = re.findall(r'(0x[\w\d]+)', url)
        if matches:
            return matches[0]
