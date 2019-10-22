class ContractAddressesCache:

    def __init__(self):
        self.contracts = set()

    def get_hits(self, addresses):
        addresses = set(addresses)
        return self.contracts & addresses

    def get_misses(self, addresses):
        addresses = set(addresses)
        return addresses - self.contracts

    def add(self, addresses):
        self.contracts.update(addresses)


contracts_addresses_cache = ContractAddressesCache()
