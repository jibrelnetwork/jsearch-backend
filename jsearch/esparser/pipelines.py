# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert


from jsearch.common import tables as t
from jsearch.common.contracts import is_erc20_compatible
from jsearch.common.tasks import process_new_verified_contract_transactions
from jsearch import settings


engine = create_engine(settings.JSEARCH_MAIN_DB)
conn = engine.connect()


class ContractPipeline(object):
    def process_item(self, item, spider):
        stmt = insert(t.contracts_t).values(**item)
        do_update_stmt = stmt.on_conflict_do_update(
            index_elements=['address'],
            set_=dict(item)
        )
        logging.info("Saving contact %s", item['address'])
        res = conn.execute(do_update_stmt)
        if is_erc20_compatible(item['abi']):
            logging.info("Contract at %s is ERC20 compatible, schedule transactions processing", item['address'])
            process_new_verified_contract_transactions.delay(item['address'])
        return item
