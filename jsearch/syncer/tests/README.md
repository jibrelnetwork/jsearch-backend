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
      - [ ] chain event with type 'created'
   2. Split chain event. 
      - [ ] All parts of forked blocks was marked as forked
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
      - [ ] All parts of canonical blocks was marked as not forked
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
      - [ ] Check splits processing can find block chains
        - [ ] Check old chain
        - [ ] Check new chain
      - [ ] Check splits applying
        - [ ] Check drop chain case
        - [ ] Check add chain case
        - [ ] Check drop and add chains case
        - [ ] Chech chain event with type 'split'
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
      - [ ] if worker have completed sync range - scaler should keep working
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
      - [ ] wallet event types recognitions 
      - [ ] transitions from transactions
      - [ ] transitions from internal transactions
   2. Logs
      - [ ] decode ERC-20 
   3. Token transfers
      - [ ] transition from logs 
   4. Assets summary
      - [ ] transition from token holder balances
      - [ ] transition from accounts states
6. Monitoring.
   1. Lags.
      - [ ] lag to etherscan
      - [ ] lag to infura
      - [ ] lag to raw db
   2. API.
      - [ ] worker returns own state 
      - [ ] worker returns healthcheck 
      - [ ] worker returns metrics
   2. Check canonical chain.
