import aiohttp
from aiohttp import web


DEFAULT_LIMIT = 20
MAX_LIMIT = 20
DEFAULT_OFFSET = 0
DEFAULT_ORDER = 'desc'


class Tag:
    """
    Block tag, can be block number, block hash or 'latest' lable
    """
    LATEST = 'latest'
    NUMBER = 'number'
    HASH = 'hash'

    __types = [LATEST, NUMBER, HASH]

    def __init__(self, type_, value):
        assert type_ in self.__types, 'Invalid tag type: {}'.format(type_)
        self.type = type_
        self.value = value

    def is_number(self):
        return self.type == self.NUMBER

    def is_hash(self):
        return self.type == self.HASH

    def is_latest(self):
        return self.type == self.LATEST


def get_tag(request):
    tag_value = request.match_info.get('tag') or request.query.get('tag', Tag.LATEST)
    if tag_value.isdigit():
        value = int(tag_value)
        type_ = Tag.NUMBER
    elif tag_value == Tag.LATEST:
        value = tag_value
        type_ = Tag.LATEST
    else:
        value = tag_value
        type_ = Tag.HASH
    return Tag(type_, value)


def validate_params(request):
    params = {}
    errors = {}

    limit = request.query.get('limit')
    if limit and limit.isdigit():
        params['limit'] = int(limit)
    elif limit and not limit.isdigit():
        errors['limit'] = 'Limit value should be valid integer, got "{}"'.format(limit)
    else:
        params['limit'] = DEFAULT_LIMIT

    offset = request.query.get('offset')
    if offset and offset.isdigit():
        params['offset'] = int(offset)
    elif offset and not offset.isdigit():
        errors['offset'] = 'Limit value should be valid integer, got "{}"'.format(offset)
    else:
        params['offset'] = DEFAULT_OFFSET

    order = request.query.get('order', '').lower()
    if order and order in ['asc', 'desc']:
        params['order'] = order
    elif order:
        errors['order'] = 'Order value should be one of "asc", "desc", got "{}"'.format(order)
    else:
        params['order'] = DEFAULT_ORDER

    if errors:
        raise web.HTTPBadRequest(errors)
    return params


async def get_account(request):
    """
    Get account by adress
    """
    storage = request.app['storage']
    address = request.match_info.get('address')
    tag = get_tag(request)

    account = await storage.get_account(address, tag)
    if account is None:
        return web.json_response(status=404)
    return web.json_response(account.to_dict())


async def get_account_transactions(request):
    """
    Get account transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address')
    params = validate_params(request)

    txs = await storage.get_account_transactions(address, params['limit'], params['offset'])
    return web.json_response([t.to_dict() for t in txs])


async def get_account_mined_blocks(request):
    """
    Get account mined blocks
    """
    storage = request.app['storage']
    address = request.match_info.get('address')

    blocks = await storage.get_account_mined_blocks(address)
    return web.json_response([b.to_dict() for b in blocks])


async def get_blocks(request):
    """
    Get blocks list
    """
    params = validate_params(request)

    storage = request.app['storage']
    blocks = await storage.get_blocks(params['limit'], params['offset'], params['order'])
    return web.json_response([block.to_dict() for block in blocks])


async def get_block(request):
    """
    Get block by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    block = await storage.get_block(tag)
    if block is None:
        return web.json_response(status=404)
    return web.json_response(block.to_dict())


async def get_block_transactions(request):
    storage = request.app['storage']
    tag = get_tag(request)
    txs = await storage.get_block_transactions(tag)
    return web.json_response([t.to_dict() for t in txs])


async def get_block_uncles(request):
    storage = request.app['storage']
    tag = get_tag(request)
    uncles = await storage.get_block_uncles(tag)
    return web.json_response([u.to_dict() for u in uncles])


async def get_transaction(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    transaction = await storage.get_transaction(txhash)
    if transaction is None:
        return web.json_response(status=404)
    return web.json_response(transaction.to_dict())


async def get_receipt(request):
    storage = request.app['storage']
    txhash = request.match_info.get('txhash')

    receipt = await storage.get_receipt(txhash)
    if receipt is None:
        return web.json_response(status=404)
    return web.json_response(receipt.to_dict())


async def get_uncles(request):
    """
    Get uncles list
    """
    params = validate_params(request)
    storage = request.app['storage']
    uncles = await storage.get_uncles(params['limit'], params['offset'], params['order'])
    return web.json_response([uncle.to_dict() for uncle in uncles])


async def get_uncle(request):
    """
    Get uncle by hash or number
    """
    storage = request.app['storage']
    tag = get_tag(request)
    uncle = await storage.get_uncle(tag)
    if uncle is None:
        return web.json_response(status=404)
    return web.json_response(uncle.to_dict())


async def call_web3_method(request):
    payload = await request.text()
    proxy_url = request.app['node_proxy_url']
    async with aiohttp.ClientSession() as session:
        async with session.post(proxy_url, data=payload) as resp:
            resp.status
            data = await resp.json()
            return web.json_response(data, status=resp.status)
