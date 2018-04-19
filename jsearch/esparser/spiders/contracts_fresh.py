import time
import logging

import scrapy

from jsearch.esparser.spiders.contracts import ContractsSpider


DELAY = 10


class FreshContractsSpider(ContractsSpider):

    name = 'contracts_fresh'
    download_delay = 1

    def get_next_page_request(self, response):
        logging.debug('Sleeping for %s sec', DELAY)
        time.sleep(DELAY)
        nextreq = scrapy.Request(url=self.listing_url, dont_filter=True, callback=self.parse_listing)
        return nextreq
