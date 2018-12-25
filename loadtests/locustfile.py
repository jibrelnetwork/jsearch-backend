import os
import random

import psycopg2
from locust import HttpLocust, TaskSet, task

keys = {}


class UserBehavior(TaskSet):

    def setup(self):
        conn = psycopg2.connect(os.getenv('JSEARCH_MAIN_DB'))
        print('Loading keys...')
        cur = conn.cursor()
        cur.execute('SELECT number, hash from blocks LIMIT 10000')
        keys['blocks'] = cur.fetchall()
        cur.execute('SELECT address from accounts_base LIMIT 10000')
        keys['accounts'] = cur.fetchall()
        cur.execute('SELECT hash from transactions LIMIT 10000')
        keys['transactions'] = cur.fetchall()
        cur.execute('SELECT number, hash from uncles LIMIT 10000')
        keys['uncles'] = cur.fetchall()
        conn.close()

        blocks = len(keys['blocks'])
        accounts = len(keys['accounts'])
        txs = len(keys['transactions'])

        print(f'Keys loaded: B: {blocks}, A: {accounts}, Tx: {txs}')

    # app.router.add_route('GET', '/accounts/balances', handlers.get_accounts_balances)
    @task(1)
    def get_accounts_balances(self):
        addresses = ','.join(random.choices(keys['accounts']), k=5)
        self.client.get(f"/accounts/balances?addresses={addresses}", catch_response=False, name="/account/balances")

    # app.router.add_route('GET', '/accounts/{address}', handlers.get_account)
    @task(1)
    def get_account(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/accounts/{address}", catch_response=False, name="/account/{address}")

    # app.router.add_route('GET', '/accounts/{address}/transactions', handlers.get_account_transactions)
    @task(1)
    def get_account_transactions(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/accounts/{address}/transactions", catch_response=False)

    # app.router.add_route('GET', '/accounts/{address}/mined_blocks', handlers.get_account_mined_blocks)
    # app.router.add_route('GET', '/accounts/{address}/mined_uncles', handlers.get_account_mined_uncles)

    # app.router.add_route('GET', '/accounts/{address}/token_transfers', handlers.get_account_token_transfers)
    @task(1)
    def get_account_token_transfers(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/accounts/{address}/token_transfers", catch_response=False)

    # app.router.add_route('GET', '/blocks', handlers.get_blocks)
    @task(1)
    def get_blocks(self):
        self.client.get("/blocks", catch_response=False)

    @task(1)
    def get_block_latest(self):
        self.client.get("/blocks/latest", catch_response=False)

    # app.router.add_route('GET', '/blocks/{tag}', handlers.get_block)
    @task(1)
    def get_block_number_rand_num(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get("/blocks/{}".format(block_num), catch_response=False, name="/blocks/{number}")

    @task(1)
    def get_block_rand_hash(self):
        block_hash = random.choice(keys['blocks'])[1]
        self.client.get("/blocks/{}".format(block_hash), catch_response=False, name="/blocks/{hash}")

    # app.router.add_route('GET', '/blocks/{tag}/transactions', handlers.get_block_transactions)
    @task(1)
    def get_block_transactions(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get(
            "/blocks/{}/transactions".format(block_num),
            catch_response=False,
            name="/blocks/{number}/transactions"
        )

    # app.router.add_route('GET', '/transactions/{txhash}', handlers.get_transaction)
    @task(1)
    def get_transaction(self):
        tx_hash = random.choice(keys['transactions'])[0]
        self.client.get(f"/transactions/{tx_hash}", catch_response=False, name="/transactions/{hash}")

    # app.router.add_route('GET', '/receipts/{txhash}', handlers.get_receipt)
    @task(1)
    def get_receipt(self):
        tx_hash = random.choice(keys['transactions'])[0]
        self.client.get(f"/receipts/{tx_hash}", catch_response=False, name="/receipts/{hash}")

    # app.router.add_route('GET', '/uncles', handlers.get_uncles)
    @task(1)
    def get_uncles(self):
        self.client.get("/uncles", catch_response=False)

    # app.router.add_route('GET', '/uncles/{tag}', handlers.get_uncle)
    @task(1)
    def get_uncle_number_rand_num(self):
        uncle_num = random.choice(keys['uncles'])[0]
        self.client.get(f"/uncles/{uncle_num}", catch_response=False, name="/uncles/{number}")

    @task(1)
    def get_uncle_rand_hash(self):
        uncle_hash = random.choice(keys['uncles'])[1]
        self.client.get(f"/uncles/{uncle_hash}", catch_response=False, name="/uncles/{hash}")

    # app.router.add_route('GET', '/tokens/{address}/transfers', handlers.get_token_transfers)
    @task(1)
    def get_account(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/tokens/{address}/transfers", catch_response=False, name="/tokens/{address}/transfers")


class ApiUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 100
    max_wait = 500
