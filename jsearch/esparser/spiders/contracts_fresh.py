import scrapy

from jsearch.esparser.spiders.contracts import ContractsSpider


class FreshContractsSpider(ContractsSpider):

	name = 'contracts_fresh'

    def get_next_page_request(self, response):
        return scrapy.Request(url=self.listing_url, dont_filter=True, callback=self.parse_listing)