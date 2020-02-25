DEX_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "orderCreator", "type": "address"},
            {"indexed": True, "name": "orderID", "type": "string"},
            {"indexed": False, "name": "orderType", "type": "uint8"},
            {"indexed": True, "name": "tradedAsset", "type": "address"},
            {"indexed": False, "name": "tradedAmount", "type": "uint256"},
            {"indexed": False, "name": "fiatAsset", "type": "address"},
            {"indexed": False, "name": "assetPrice", "type": "uint256"},
            {"indexed": False, "name": "expirationTimestamp", "type": "uint256"},
        ],
        "name": "OrderPlacedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "orderID", "type": "string"}],
        "name": "OrderActivatedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "orderID", "type": "string"}],
        "name": "OrderCompletedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "orderID", "type": "string"}],
        "name": "OrderCancelledEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "orderID", "type": "string"}],
        "name": "OrderExpiredEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "tradeCreator", "type": "address"},
            {"indexed": True, "name": "tradeID", "type": "uint256"},
            {"indexed": True, "name": "orderID", "type": "string"},
            {"indexed": False, "name": "tradedAmount", "type": "uint256"},
        ],
        "name": "TradePlacedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "tradeID", "type": "uint256"}],
        "name": "TradeCompletedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [{"indexed": True, "name": "tradeID", "type": "uint256"}],
        "name": "TradeCancelledEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "userAddress", "type": "address"},
            {"indexed": True, "name": "assetAddress", "type": "address"},
            {"indexed": False, "name": "assetAmount", "type": "uint256"},
        ],
        "name": "TokensBlockedEvent",
        "type": "event",
    },
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "userAddress", "type": "address"},
            {"indexed": True, "name": "assetAddress", "type": "address"},
            {"indexed": False, "name": "assetAmount", "type": "uint256"},
        ],
        "name": "TokensUnblockedEvent",
        "type": "event",
    },
]
