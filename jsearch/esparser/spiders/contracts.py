import json
import datetime

import scrapy
from scrapy.linkextractors import LinkExtractor
from w3lib.html import remove_tags

from jsearch.esparser.items import ContractItem


class ContractsSpider(scrapy.Spider):
    name = 'contracts'
    allowed_domains = ['etherscan.io']

    listing_url = 'https://etherscan.io/contractsVerified'

    def start_requests(self):
        yield scrapy.Request(url=self.listing_url, callback=self.parse_listing, meta={'page': 1})

    def parse_listing(self, response):
        links = LinkExtractor(allow='\/address\/0x.*', restrict_css='.table-responsive').extract_links(response)
        dates = response.css('.table-responsive tr td:last-child::text').extract()
        for link, date in zip(links, dates):
            yield scrapy.Request(url=link.url, callback=self.parse_contract, meta={'date_verified': date})
        if len(dates) > 0:
            yield self.get_next_page_request(response)

    def get_next_page_request(self, response):
        next_page = response.meta['page'] + 1
        return scrapy.Request(url=self.listing_url + '/{}'.format(next_page),
                              callback=self.parse_listing,
                              meta={'page': next_page})

    def parse_contract(self, response):
        """
            {'Contract Name:': 'MDAPPSale',
             'Compiler Text:': 'v0.4.25+commit.59dbf8f1',
             'Optimization Enabled:': 'Yes',
             'Runs (Optimiser):': '200'}


          Item:
            name = scrapy.Field()
            compiler_version = scrapy.Field()
            optimization_enabled = scrapy.Field()
            optimization_runs = scrapy.Field()

            source_code = scrapy.Field()
            byte_code = scrapy.Field()
            abi = scrapy.

        """
        item = ContractItem()

        item['address'] = response.css('#mainaddress::text').extract()[0].lower()
        item['source_code'] = response.css('.js-sourcecopyarea::text').extract()[0]
        item['abi'] = json.loads(response.css('.js-copytextarea2::text').extract()[0])
        item['byte_code'] = response.css('#verifiedbytecode2::text').extract()[0]
        item['verified_at'] = datetime.datetime.strptime(response.meta['date_verified'], '%m/%d/%Y')

        # compile params
        parts = response.css('#code').css("table.table td").extract()
        parts = [remove_tags(p).strip().replace('\xa0', ' ') for p in parts]
        compile_params = dict(zip(parts[::2], parts[1::2]))

        item['name'] = compile_params['Contract Name:']
        item['compiler_version'] = compile_params['Compiler Text:']
        if compile_params['Optimization Enabled:'] == 'Yes':
            item['optimization_enabled'] = True
        elif compile_params['Optimization Enabled:'] == 'No':
            item['optimization_enabled'] = False
        else:
            raise AssertionError('Optimization Enabled: invalid value {}'.format(
                compile_params['Optimization Enabled:']))
        item['optimization_runs'] = compile_params['Runs (Optimiser):']

        item['grabbed_at'] = datetime.datetime.now()

        yield item
