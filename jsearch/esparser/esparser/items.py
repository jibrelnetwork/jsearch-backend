# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ContractItem(scrapy.Item):
    address = scrapy.Field()
    name = scrapy.Field()
    compiler_version = scrapy.Field()
    optimization_enabled = scrapy.Field()
    optimization_runs = scrapy.Field()

    source_code = scrapy.Field()
    byte_code = scrapy.Field()
    abi = scrapy.Field()

    def __repr__(self):
        return 'Contract {} at {}'.format(self['name'], self['address'])
