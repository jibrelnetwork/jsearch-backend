
import logging

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from celery.signals import celeryd_init

from jsearch.esparser.spiders.contracts import ContractsSpider
from jsearch.esparser import settings as scrapy_settings
from jsearch.common.celery import app


logger = logging.getLogger(__name__)


@app.task
def run_contracts_spider():
    logger.info('Start Contracts Spider')
    st = get_project_settings()
    st.setmodule(scrapy_settings, 'project')
    process = CrawlerProcess(st)

    process.crawl(ContractsSpider)
    process.start()
    logger.info('Stop Contracts Spider')


@celeryd_init.connect()
def start_contract_spider_task(conf=None, **kwargs):
    run_contracts_spider.delay()
