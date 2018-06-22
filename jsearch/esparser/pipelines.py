# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert


from jsearch.common import tables as t
from jsearch.common.tasks import process_new_verified_contract_transactions

engine = create_engine(os.environ['JSEARCH_MAIN_DB'], echo=True)
conn = engine.connect()


class ContractPipeline(object):
    def process_item(self, item, spider):
        stmt = insert(t.contracts_t).values(**item)
        do_update_stmt = stmt.on_conflict_do_update(
            index_elements=['address'],
            set_=dict(item)
        )

        conn.execute(do_update_stmt)
        process_new_verified_contract_transactions.delay(item['address'])
        return item
