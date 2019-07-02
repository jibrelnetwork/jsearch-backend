import pytest


@pytest.fixture(scope="session")
def transfer_on_fuck_token_contract(w3, fuck_token) -> int:
    default_account = w3.client.eth.accounts[0]
    w3.client.eth.defaultAccount = default_account

    account_sender = w3.client.personal.newAccount('password')
    account_receiver = w3.client.personal.newAccount('password')

    for account in w3.client.eth.accounts:
        w3.client.personal.unlockAccount(account=account, passphrase='password')

    contract = w3.client.eth.contract(
        abi=fuck_token.abi_as_dict(),
        bytecode=fuck_token.bin,
    )
    tx_hash = contract.constructor().transact({
        "from": w3.client.eth.accounts[0],
        "gas": 1000000,
    })

    w3.mine_block()
    tx_receipt = w3.client.eth.waitForTransactionReceipt(tx_hash)

    contract_instance = w3.client.eth.contract(
        address=tx_receipt.contractAddress,
        abi=fuck_token.abi
    )
    tx_hash = contract_instance.functions.transfer(
        to=account_receiver,
        tokens=1000
    ).transact({
        "gas": 100000,
    })
    w3.mine_block()
    w3.client.eth.waitForTransactionReceipt(tx_hash)

    tx_hash = contract_instance.functions.approve(
        spender=account_sender,
        tokens=500
    ).transact({
        "gas": 100000
    })
    w3.mine_block()
    w3.client.eth.waitForTransactionReceipt(tx_hash)

    tx_hash = contract_instance.functions.transferFrom(
        account_receiver,
        to=account_sender,
        tokens=400,
    ).transact({
        "gas": 100000
    })
    w3.mine_block()
    w3.client.eth.waitForTransactionReceipt(tx_hash)
    mined_blocks = []
    for n in range(w3.last_block_number):
        mined_blocks.append(w3.client.eth.getBlock(n)['hash'].hex())
    return mined_blocks
