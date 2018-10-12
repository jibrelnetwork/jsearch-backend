
import logging
import io
import itertools
import json
import datetime
import random
import threading
import time
import queue

from lxml import etree
from cssselect import GenericTranslator
import requests
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert

from jsearch.common import tables as t
from jsearch.common.contracts import is_erc20_compatible
from jsearch.common.tasks import process_new_verified_contract_transactions
from jsearch.esparser import settings


logger = logging.getLogger(__name__)

BASE_URL = 'https://etherscan.io'
CONTRACT_LIST_URL = BASE_URL + '/contractsVerified'
FETCH_RETRIES = 5
DB_WRITE_QUEUE_MAXSIZE = 1000
DB_WRITE_QUEUE_WATERMARK = DB_WRITE_QUEUE_MAXSIZE / 2


def mk_proxy_url(host):
    return 'https://{user}:{passwd}@{host}'.format(
        user=settings.PROXY_USER, passwd=settings.PROXY_PASS, host=host)


_proxy_list = [{'host': mk_proxy_url(p), 'OK': 0, 'ERR': 0} for p in settings.PROXY_LIST]
_proxy_cycle = itertools.cycle(_proxy_list)


def xp(css_selector):
    return GenericTranslator().css_to_xpath(css_selector)


class Service:
    """
    Component container
    """
    def __init__(self, options):
        self.options = options
        # self.main_db = MainDB(settings.JSEARCH_MAIN_DB)
        self.fresh_parse_thread = threading.Thread(target=self.fresh_parse)
        self.deep_parse_thread = threading.Thread(target=self.deep_parse)
        self.process_thread = threading.Thread(target=self.process_contracts_queue)
        self.db_write_queue = queue.Queue(maxsize=DB_WRITE_QUEUE_MAXSIZE)
        engine = create_engine(settings.JSEARCH_MAIN_DB)
        self.db_conn = engine.connect()
        self._running = False

    def run(self):
        """
        Start all process
        """
        logger.info("Starting jSearch ESparser")
        self._running = True
        self.fresh_parse_thread.start()
        self.deep_parse_thread.start()
        self.process_thread.start()

        self.fresh_parse_thread.join()
        self.deep_parse_thread.join()
        self.process_thread.join()

    def fresh_parse(self):
        logger.info('Starting Fresh parse thread')
        while self._running:
            time.sleep(0.5)
            self.parse_contracts_list_page()
        logger.info('Fresh parse thread is stopped')

    def deep_parse(self):
        logger.info('Starting Deep parse thread')
        p = 0
        while self._running:
            time.sleep(0.5)
            contracts_num = self.parse_contracts_list_page(p)
            if contracts_num == 0:
                p = 0
            else:
                p += 1
        logger.info('Deep parse thread is stopped')

    def process_contracts_queue(self):
        logger.info('Starting process thread')
        while self._running:
            contract = self.db_write_queue.get()
            try:
                self.process_contract(contract)
            except:
                logger.exception('Contract write error')
                self.db_write_queue.put(contract)
                time.sleep(5)

        logger.info('Process thread is stopped')

    def parse_contracts_list_page(self, page=0):
        list_url = CONTRACT_LIST_URL + (str(page) if page > 0 else '')
        contracts_list_page = self.fetch(list_url)
        if contracts_list_page is None:
            logger.warn('Unable to fetch list %s, skipping', list_url)
            return 0
        try:
            contracts_urls = self.parse_list(contracts_list_page)
        except Exception:
            logger.exception('Contracts list %s parse error, skipping', list_url)
            return 0
        for url, date in contracts_urls:
            if self._running is False:
                break
            time.sleep(0.5)
            contract_page = self.fetch(BASE_URL + url)
            if contract_page is None:
                logger.warn('Unable to fetch contract %s, skipping', url)
                continue
            try:
                contract = self.parse_contract(contract_page)
                contract['verified_at'] = datetime.datetime.strptime(date, '%m/%d/%Y')
                logger.debug('Contract parsed: %s  at %s', contract['name'], contract['address'])
                self.db_write_queue.put(contract)
            except Exception:
                logger.exception('Contract %s parse error, skipping', url)
                continue
        return len(contracts_urls)

    def fetch(self, url, retry=FETCH_RETRIES):
        proxy = self.get_proxy()
        ua = random.choice(settings.USER_AGENT_LIST).strip()
        headers = {'user-agent': ua}
        logger.debug('Fetching URL %s, retry %s', url, FETCH_RETRIES - retry)
        try:
            resp = requests.get(url, proxies={'https': proxy['host']}, headers=headers)
            logger.debug('HTTP status: %s', resp.status_code)
            proxy['OK'] += 1
            return resp.text
        except Exception:
            logger.exception('URL Fetching error:')
            proxy['ERR'] += 1
            if retry > 0:
                time.sleep(1)
                return self.fetch(url, retry - 1)

    def parse_contract(self, page):
        et = self._parse_html(page)
        item = {}

        item['address'] = et.xpath(xp('#mainaddress'))[0].text.lower()
        item['source_code'] = et.xpath(xp('.js-sourcecopyarea'))[0].text
        item['abi'] = json.loads(et.xpath(xp('.js-copytextarea2'))[0].text)
        item['byte_code'] = et.xpath(xp('#verifiedbytecode2'))[0].text

        # compile params
        parts = [e.text.replace('\xa0', ' ').strip() for e in et.xpath(xp('#code table.table td'))]

        compile_params = dict(zip(parts[::2], parts[1::2]))

        item['name'] = compile_params['Contract']
        item['compiler_version'] = compile_params['Compiler']
        if compile_params['Optimization'] == 'Yes':
            item['optimization_enabled'] = True
        elif compile_params['Optimization'] == 'No':
            item['optimization_enabled'] = False
        else:
            raise AssertionError('Optimization Enabled: invalid value {}'.format(
                compile_params['Optimization']))
        item['optimization_runs'] = compile_params['Runs (Optimiser):']
        item['grabbed_at'] = datetime.datetime.now()
        return item

    def parse_list(self, page):
        et = self._parse_html(page)
        els = et.xpath(xp('.table-responsive tr td:last-child'))
        dates = [e.text for e in els]
        hrefs = [el.attrib['href'] for el in et.xpath('.//table//a')]
        return list(zip(hrefs, dates))

    def _parse_html(self, html):
        parser = etree.HTMLParser()
        tree = etree.parse(io.StringIO(html), parser)
        return tree

    def get_proxy(self):
        proxy = next(_proxy_cycle)
        return proxy

    def stop(self):
        logger.info("Stopping jSearch ESparser")
        logger.info("finishing jobs...")
        self._running = False

    def process_contract(self, item):
        q = t.contracts_t.select().where(t.contracts_t.c.address == item['address'])
        insert_stmt = insert(t.contracts_t).values(**item)
        update_stmt = t.contracts_t.update().where(t.contracts_t.c.address == item['address']).values(**item)

        contract = self.db_conn.execute(q).fetchone()
        if contract is None:
            logger.info("Inserting contact %s", item['address'])
            self.db_conn.execute(insert_stmt)
        elif contract['grabbed_at'] is not None:
            logger.info("Updating contact %s", item['address'])
            self.db_conn.execute(update_stmt)
        else:
            logger.info("Contact %s verifid by jSearch, skipping", item['address'])
            return item

        if is_erc20_compatible(item['abi']):
            logger.info("Contract at %s is ERC20 compatible, schedule transactions processing", item['address'])
            process_new_verified_contract_transactions.delay(item['address'])
        return item
