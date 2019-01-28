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
        cur.execute('SELECT number, hash from blocks ORDER BY number DESC LIMIT 10000')
        keys['blocks'] = cur.fetchall()
        cur.execute('SELECT address from accounts_state ORDER BY block_number DESC LIMIT 10000')
        keys['accounts'] = cur.fetchall()
        cur.execute('SELECT hash from transactions ORDER BY block_number DESC LIMIT 10000')
        keys['transactions'] = cur.fetchall()
        cur.execute('SELECT number, hash from uncles ORDER BY block_number DESC LIMIT 10000')
        keys['uncles'] = cur.fetchall()
        conn.close()

        blocks = len(keys['blocks'])
        accounts = len(keys['accounts'])
        txs = len(keys['transactions'])

        print(f'Keys loaded: B: {blocks}, A: {accounts}, Tx: {txs}')

    # app.router.add_route('GET', '/v1/accounts/balances', handlers.get_accounts_balances)
    @task(1)
    def get_accounts_balances(self):
        addresses = ','.join([r[0] for r in random.choices(keys['accounts'], k=5)])
        self.client.get(f"/v1/accounts/balances?addresses={addresses}", catch_response=False, name="/v1/account/balances")

    # app.router.add_route('GET', '/v1/accounts/{address}', handlers.get_account)
    @task(1)
    def get_account(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/v1/accounts/{address}", catch_response=False, name="/v1/account/{address}")

    # app.router.add_route('GET', '/v1/accounts/{address}/transactions', handlers.get_account_transactions)
    @task(1)
    def get_account_transactions(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/v1/accounts/{address}/transactions",
                        catch_response=False,
                        name='/v1/accounts/{address}/transactions')

    # app.router.add_route('GET', '/v1/accounts/{address}/mined_blocks', handlers.get_account_mined_blocks)
    # app.router.add_route('GET', '/v1/accounts/{address}/mined_uncles', handlers.get_account_mined_uncles)

    # app.router.add_route('GET', '/v1/accounts/{address}/token_transfers', handlers.get_account_token_transfers)
    @task(1)
    def get_account_token_transfers(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/v1/accounts/{address}/token_transfers",
                        catch_response=False,
                        name='/v1/accounts/{}/token_transfers')

    # app.router.add_route('GET', '/v1/blocks', handlers.get_blocks)
    @task(1)
    def get_blocks(self):
        self.client.get("/v1/blocks", catch_response=False)

    @task(1)
    def get_block_latest(self):
        self.client.get("/v1/blocks/latest", catch_response=False)

    # app.router.add_route('GET', '/v1/blocks/{tag}', handlers.get_block)
    @task(1)
    def get_block_number_rand_num(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get("/v1/blocks/{}".format(block_num), catch_response=False, name="/v1/blocks/{number}")

    @task(1)
    def get_block_rand_hash(self):
        block_hash = random.choice(keys['blocks'])[1]
        self.client.get("/v1/blocks/{}".format(block_hash), catch_response=False, name="/v1/blocks/{hash}")

    # app.router.add_route('GET', '/v1/blocks/{tag}/transactions', handlers.get_block_transactions)
    @task(1)
    def get_block_transactions(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get(
            "/v1/blocks/{}/transactions".format(block_num),
            catch_response=False,
            name="/v1/blocks/{number}/transactions"
        )

    # app.router.add_route('GET', '/v1/transactions/{txhash}', handlers.get_transaction)
    @task(1)
    def get_transaction(self):
        tx_hash = random.choice(keys['transactions'])[0]
        self.client.get(f"/v1/transactions/{tx_hash}", catch_response=False, name="/v1/transactions/{hash}")

    # app.router.add_route('GET', '/v1/receipts/{txhash}', handlers.get_receipt)
    @task(1)
    def get_receipt(self):
        tx_hash = random.choice(keys['transactions'])[0]
        self.client.get(f"/v1/receipts/{tx_hash}", catch_response=False, name="/v1/receipts/{hash}")

    # app.router.add_route('GET', '/v1/uncles', handlers.get_uncles)
    @task(1)
    def get_uncles(self):
        self.client.get("/v1/uncles", catch_response=False)

    # app.router.add_route('GET', '/v1/uncles/{tag}', handlers.get_uncle)
    @task(1)
    def get_uncle_number_rand_num(self):
        uncle_num = random.choice(keys['uncles'])[0]
        self.client.get(f"/v1/uncles/{uncle_num}", catch_response=False, name="/v1/uncles/{number}")

    @task(1)
    def get_uncle_rand_hash(self):
        uncle_hash = random.choice(keys['uncles'])[1]
        self.client.get(f"/v1/uncles/{uncle_hash}", catch_response=False, name="/v1/uncles/{hash}")

    # app.router.add_route('GET', '/v1/tokens/{address}/transfers', handlers.get_token_transfers)
    @task(1)
    def get_token_transfers(self):
        address = ','.join(random.choice(keys['accounts']))
        self.client.get(f"/v1/tokens/{address}/transfers", catch_response=False, name="/v1/tokens/{address}/transfers")


class ApiUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 100
    max_wait = 500
