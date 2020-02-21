Design of syncer unit tests:
---------------------------

1. Sync the chain event
   1. Create chain event
      - [x] Block was recorded to db. 
          - [x] a block data
          - [x] uncles
          - [x] transactions
          - [x] receipts
          - [x] logs
          - [x] accounts states
          - [x] accounts base
          - [x] internal transactions
          - [x] token holders
          - [x] token transfers
          - [x] wallet events
          - [x] assets summary
          - [x] assets summary pairs
          - [x] chain event with type 'created'
      - [ ] Block can't be replaces without re-sync mode 
   2. Split chain event. 
      - [x] All parts of forked blocks was marked as forked
          - [x] a block data
          - [x] uncles
          - [x] transactions
          - [x] receipts
          - [x] logs
          - [x] accounts states
          - [x] internal transactions
          - [x] token holders
          - [x] token transfers
          - [x] wallet events
          - [x] assets summary
      - [x] All parts of canonical blocks was marked as not forked
          - [x] a block data
          - [x] uncles
          - [x] transactions
          - [x] receipts
          - [x] logs
          - [x] accounts states
          - [x] internal transactions
          - [x] token holders
          - [x] token transfers
          - [x] wallet events
          - [x] assets summary
      - [ ] Check splits applying
        - [ ] Check new chain are in canonical chain
        - [ ] Check old chain are not in canonical chain  
        - [ ] Check chain event with type 'split' is recorded 
   3. Reinserted events
      - [ ] Reinserted events writes to db
      - [ ] Reinserted events don't affected any data
   4. Resync mode
      - [ ] block rewriting 
      - [ ] split re-applying
        - [ ] Check all missed create events are synced
        - [ ] Check all missed splits events are synced
        - [ ] resync-chain-splits mode on/off
   5. Switch between nodes. 
      - [ ] 2 nodes configuration - left, right. Left node has no more events to sync.
          - [x] right has and it is in canonical chain
          - [x] right has and it is in fork chain 
          - [x] right has, there are not common blocks between left and right nodes
          - [x] right has, there is a gap between nodes
          - [x] right has no events
      - [ ] Timeout check.
          - [ ] switch exactly after timeout
          - [ ] timeout works only on open range (from 0 until -)
      - [ ] multiple nodes configuration - left and 3 or more. Left node has not more events to sync.
          - [ ] there are tree nodes with different chains, get node with the longest chain
          - [ ] there are tree nodes with different chains, the longest node has not common block with left
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
      - [x] wallet event types recognitions 
      - [ ] transitions from transactions
      - [ ] transitions from internal transactions
   2. Dex
      - [ ] decode ERC-20 
      - [ ] decode DEX events
   3. Token transfers
      - [x] transition from logs 
   4. Assets summary
      - [ ] transition from token holder balances
      - [ ] transition from accounts states
   5. DEX events
      - [x] transition from logs
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
