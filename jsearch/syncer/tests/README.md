Desing of syncer unit tests:
------------------

1. Sync the chain event
   1. Create chain event
      - [ ] a block data
      - [ ] an uncles
      - [ ] transactions
      - [ ] receipts
      - [ ] logs
      - [ ] accounts states
      - [ ] accounts base
      - [ ] internal transactions
      - [ ] token holders
      - [ ] token transfers
      - [ ] wallet events
      - [ ] assets summary
   2. Split chain event
      - [ ] old blocks are forked and new blocks are canonical
   3. Resync mode
      - [ ] block rewriting 
      - [ ] split re-applying
      - [ ] resync-chain-splits mode on/off
2. Test the cli 
   1 One worker entrypoints
     - [ ] sync range validation
     - [ ] resync mode
     - [ ] resync-chain-splits mode 
   2. Scaler entrypoints
     - [ ] workers count
3. Scaler
   1. Rescale worker counts.
      - [ ] Scalling kills hanged workers.
      - [ ] Scalling runs new workers
      - [ ] Scalling runs only once.
   2. API
      - [ ] Scaler returns state for all workers.
   3. Behavior:
      - [ ] if one worker stops without exit code 0 - scaler will stops with exit code 1
4. Behavior
   1. Exit
       - [ ] Error have occurred, then worker should exits with code 1.
       - [ ] Block range have synced, then worker should stop with code 0.
   2. Recovery from error strategy.
       - [ ] Reconnect to RAW DB in case when connection was lost [only 3 times].
       - [ ] Reconnect to MAIN DB in case when connection was lost [only 3 times].
       - [ ] Error raises due to insert procedure calling.
5. Processing.
   1. Wallet events 
      - [ ] types recognitions 
   2. Logs
      - [ ] transition to transfers
   3. Transactions
      - [ ] transition to wallet events
   4. Internal transactions
      - [ ] transition to wallet events
   6. Token transfers
      - [ ] transition to wallet events 
   7. Token holders
      - [ ] transition to assets summary
   8. Accounts states
      - [ ] transition to assets summary
6. Monitoring.
   1. Lags.
      - [ ] lag to etherscan
      - [ ] lag to infura
      - [ ] lag to raw db
   2. API.
      - [ ] worker returns own state 
      - [ ] worker returns healthcheck 
      - [ ] worker returns metrics
 
