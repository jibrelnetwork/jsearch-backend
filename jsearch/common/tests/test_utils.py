from jsearch.common.contracts import cut_contract_metadata_hash


def test_cut_contract_metadata_hash_noop():
    b = '490208054869003905583518581529351929391927fddf252ad1be2c89b'\
        '69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef9281900390910190a35b9056'

    bcode, mhash = cut_contract_metadata_hash(b)
    assert b == bcode
    assert '' == mhash


def test_cut_contract_metadata_hash_normal():
    b = '183811015610e6357fe5b93925050505600'\
        'a165627a7a72305820'\
        'f47a61058abe74d2f7ce28152f266816f6790e0080c4e28ce834583bb579bf000029'\
        'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'\
        '000000000000000000000000f1c26ab70d0303dc62e17b4f96b862f21827f069'

    bcode, mhash = cut_contract_metadata_hash(b)
    assert bcode == b.replace('f47a61058abe74d2f7ce28152f266816f6790e0080c4e28ce834583bb579bf00', '')
    assert mhash == 'f47a61058abe74d2f7ce28152f266816f6790e0080c4e28ce834583bb579bf00'


def test_cut_contract_metadata_hash_experimental():
    b = 'ffffffffffffffe016905600'\
        'a265627a7a72305820'\
        '8235d45abdffaae20233092034ccb050ee6aa0359cc7cf'\
        'e883297367865e2a8a6c6578706572696d656e74616cf50037'\
        'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'\
        '000000000000000000000000a8a22f42274bdd66b65abc8510fd808176f80cca'\

    bcode, mhash = cut_contract_metadata_hash(b)
    assert bcode == b.replace('8235d45abdffaae20233092034ccb050ee6aa0359cc7cfe883297367865e2a8a', '')
    assert mhash == '8235d45abdffaae20233092034ccb050ee6aa0359cc7cfe883297367865e2a8a'
