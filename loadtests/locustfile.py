import os
import random

from locust import HttpLocust, TaskSet, task
import psycopg2


keys = {}


class UserBehavior(TaskSet):

    def setup(self):
        conn = psycopg2.connect(os.getenv('JSEARCH_MAIN_DB'))
        print('Loading keys...')
        cur = conn.cursor()
        cur.execute('SELECT number, hash from blocks LIMIT 10000')
        keys['blocks'] = cur.fetchall()
        cur.execute('SELECT address from accounts LIMIT 10000')
        keys['accounts'] = cur.fetchall()
        cur.execute('SELECT hash from transactions LIMIT 10000')
        keys['transactions'] = cur.fetchall()
        conn.close()
        print('Keys loaded: B: {}, A: {}, Tx: {}'.format(len(keys['blocks']), len(keys['accounts']), len(keys['transactions'])))

    @task(1)
    def get_block_latest(self):
        self.client.get("/blocks/latest", catch_response=False)

    @task(1)
    def get_block_number_rand_num(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get("/blocks/{}".format(block_num), catch_response=False, name="/blocks/{number}")

    @task(1)
    def get_block_rand_hash(self):
        block_hash = random.choice(keys['blocks'])[1]
        self.client.get("/blocks/{}".format(block_hash), catch_response=False, name="/blocks/{hash}")

    @task(1)
    def get_block_transactions(self):
        block_num = random.choice(keys['blocks'])[0]
        self.client.get("/blocks/{}/transactions".format(block_num), catch_response=False, name="/blocks/{number}/transactions")


class ApiUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 100
    max_wait = 500