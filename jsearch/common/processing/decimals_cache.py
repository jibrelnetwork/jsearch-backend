
from jsearch.common.processing.erc20_balances import get_decimals


class DecimalsCache:

    def __init__(self):
        self.storage = {}

    async def get_many(self, addresses):
        missed = []
        decimals = {}
        for addr in addresses:
            value = self.storage.get(addr)
            if value is None:
                missed.append(addr)
            else:
                decimals[addr] = value
        if missed:
            fetched = await self.fetch_many(missed)
            decimals.update(fetched)
            self.storage.update(fetched)
        return decimals

    async def fetch_many(self, addresses):
        res = await get_decimals(addresses, 20)
        return res


decimals_cache = DecimalsCache()