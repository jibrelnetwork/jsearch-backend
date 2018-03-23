from locust import HttpLocust, TaskSet, task


class UserBehavior(TaskSet):

    @task(1)
    def get_block_latest(self):
        self.client.get("/blocks/latest")

    @task(1)
    def get_block_number_100000(self):
        self.client.get("/blocks/100000")

    @task(1)
    def get_block_number_2000000(self):
        self.client.get("/blocks/2000000")

    @task(1)
    def get_block_0xc0f4906fea23cf6f3cce98cb44e8e1449e455b28d684dfa9ff65426495584de6(self):
        self.client.get("/blocks/0xc0f4906fea23cf6f3cce98cb44e8e1449e455b28d684dfa9ff65426495584de6")

    @task(1)
    def get_block_0x91c90676cab257a59cd956d7cb0bceb9b1a71d79755c23c7277a0697ccfaf8c4(self):
        self.client.get("/blocks/0x91c90676cab257a59cd956d7cb0bceb9b1a71d79755c23c7277a0697ccfaf8c4")

    @task(1)
    def get_block_transactions(self):
        self.client.get("/blocks/0x8e38b4dbf6b11fcc3b9dee84fb7986e29ca0a02cecd8977c161ff7333329681e/transactions")

    @task(1)
    def get_account(self):
        self.client.get("/accounts/0x2a65aca4d5fc5b5c859090a6c34d164135398226")

    # @task(1)
    # def get_block_latest(self):
    #     self.client.get("/blocks/latest")

    # @task(1)
    # def get_block_latest(self):
    #     self.client.get("/blocks/latest")

    # @task(2)
    # def profile(self):
    #     self.client.get("/profile")


class ApiUser(HttpLocust):
    task_set = UserBehavior
    min_wait = 5000
    max_wait = 9000
