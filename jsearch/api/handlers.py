from aiohttp import web


async def get_account(request):
    """
    Get account by adress
    """
    storage = request.app['storage']
    address = request.match_info.get('address')

    account = await storage.get_account(address)
    if account is None:
        return web.json_response(status=404)
    return web.json_response(account.to_dict())


async def get_account_transactions(request):
    """
    Get account transactions
    """
    storage = request.app['storage']
    address = request.match_info.get('address')

    txs = await storage.get_account_transactions(address)
    return web.json_response([t.to_dict() for t in txs])


async def get_block(request):
    """
    Get block by hash or number
    """
    storage = request.app['storage']
    hash_or_number = request.match_info.get('hashOrNumber')
    if hash_or_number.isdigit():
        number = int(hash_or_number)
        block_hash = None
    else:
        block_hash = hash_or_number
        number = None
    block = await storage.get_block(number=number, hash=block_hash)
    if block is None:
        return web.json_response(status=404)
    return web.json_response(block.to_dict())

    
async def get_block_transactions(request):
    return web.json_response(status=501)
    

async def get_block_uncles(request):
    return web.json_response(status=501)
    

async def get_transaction(request):
    return web.json_response(status=501)
    

async def get_receipt(request):
    return web.json_response(status=501)
    

async def call_web3_method(request):
    return web.json_response(status=501)
    
