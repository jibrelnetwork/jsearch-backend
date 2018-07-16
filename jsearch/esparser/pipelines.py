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


logger = logging.getLogger(__name__)

engine = create_engine(settings.JSEARCH_MAIN_DB)
conn = engine.connect()


class ContractPipeline(object):
    def process_item(self, item, spider):
        q = t.contracts_t.select().where(t.contracts_t.c.address == item['address'])
        insert_stmt = insert(t.contracts_t).values(**item)
        update_stmt = t.contracts_t.update().where(t.contracts_t.c.address == item['address']).values(**item)
        
        contract = conn.execute(q).fetchone()
        if contract is None:
            logger.info("Inserting contact %s", item['address'])
            conn.execute(insert_stmt)
        elif contract['grabbed_at'] is not None:
            logger.info("Updating contact %s", item['address'])
            conn.execute(update_stmt)
        else:
            logger.info("Contact %s verifid by jSearch, skipping", item['address'])
            return item

        if is_erc20_compatible(item['abi']):
            logger.info("Contract at %s is ERC20 compatible, schedule transactions processing", item['address'])
            process_new_verified_contract_transactions.delay(item['address'])
        return item
