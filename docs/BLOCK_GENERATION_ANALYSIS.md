# Block Generation Discrepancy Analysis

## Problem Statement
为什么有的节点产出一百多个区块有的节点只生成三四十个区块，什么原因造成这种差异？

Why do some nodes produce 100+ blocks while others only produce 30-40 blocks?

## Analysis

### Block Generation Mechanism

Based on the codebase analysis, block generation in this blockchain emulator follows the PBFT (Practical Byzantine Fault Tolerance) consensus protocol:

#### 1. Block Generation Process
- **Location**: `chain/blockchain.go:166-191` - `GenerateBlock(miner int32)`
- **Frequency**: Controlled by `params.Block_Interval` (default: 5000ms / 5 seconds)
- **Leader-based**: Only the current PBFT leader proposes new blocks
- **Process Flow**:
  1. Leader waits for Block_Interval
  2. Packs transactions from the transaction pool
  3. Creates BlockHeader with parent hash, block number, timestamp
  4. Builds state root from transactions
  5. Initiates PBFT consensus (PrePrepare → Prepare → Commit phases)
  6. Block is added to chain after 2f+1 confirmations

#### 2. Key Factors Affecting Block Production

##### A. Leader Rotation (Primary Factor)
- PBFT uses a rotating leader/primary node mechanism
- Only the **current leader** can propose blocks
- In a shard with N consensus nodes:
  - If there are 3 consensus nodes per shard, each node should be leader ~33% of the time
  - If view changes occur, leadership rotates to next node
  - **Result**: Different nodes produce different numbers of blocks based on:
    - How often they were leader
    - How long they held leadership
    - Whether view changes interrupted their leadership

##### B. View Changes
- **Location**: `consensus_shard/pbft_all/messageHandle.go`
- **Trigger Conditions**:
  - Leader failure detection
  - Consensus timeout (`PbftViewChangeTimeOut`)
  - Network partitions
- **Impact**: When view changes occur:
  - Current leader loses ability to propose
  - New leader takes over
  - This redistributes block production opportunities

##### C. Takeover Events (Reputation-Based)
- **Location**: `supervisor/supervisor.go:296-438`
- **Mechanism**: Traffic redirection based on node/shard health
- **Process**:
  1. Supervisor detects faulty shard via `SupervisionTimeout` (2000ms)
  2. Calculates load: `L_new = poolSum / c_dynamic`
  3. If `L_new ≤ TakeoverThreshold` (1.5): Single shard takeover
  4. If `L_new > TakeoverThreshold`: BSG (Back-up Supervisor Group) takeover
  5. Traffic redirected to healthy shard(s)
- **Impact on Block Production**:
  - **Takeover Shard**: Receives more transactions → Produces more blocks
  - **Failed Shard**: Stops producing blocks during failure period
  - **BSG Shards**: Share the load → Produce additional blocks

##### D. Transaction Pool Size
- Shards with more transactions in their pool will produce blocks more consistently
- Empty transaction pools may result in skipped blocks
- Cross-shard transaction patterns affect pool sizes differently

##### E. Epoch Duration and Reconfiguration
- **Parameter**: `params.ReconfigTimeGap`
- Network reconfiguration may temporarily affect block production
- Different shards may have different uptime during reconfig events

### Specific Causes for 100+ vs 30-40 Block Discrepancy

#### Scenario 1: Takeover Events
```
Node A (100+ blocks):
- Normal production: 50 blocks (own traffic)
- During Shard B failure: +30 blocks (takeover Shard B's traffic)
- During Shard C failure: +25 blocks (BSG member for Shard C)
Total: 105 blocks

Node B (30-40 blocks):
- Normal production: 35 blocks
- Failed for period: -10 blocks (timeout/crash)
- Recovered: +5 blocks
Total: 30 blocks
```

#### Scenario 2: Unequal Leadership Duration
```
In a 3-node shard over 200 block intervals:
- Node 0 as leader: 100 blocks (50% time - stable, no failures)
- Node 1 as leader: 60 blocks (30% time - some view changes)
- Node 2 as leader: 40 blocks (20% time - frequent view changes due to network issues)
```

#### Scenario 3: Transaction Load Imbalance
- **High-traffic shards**:
  - Receive more transactions from committee modules
  - Produce blocks consistently at Block_Interval rate
  - Result: 100+ blocks in experiment duration
- **Low-traffic shards**:
  - Sparse transaction injection
  - May skip block production when pool is empty
  - Result: 30-40 blocks in same duration

### Data Collection Points

The current system records block data in:
- **File**: `expTest/result/pbft_shardNum=N/ShardXX.csv`
- **Columns**:
  - Block Height
  - EpochID
  - TxPool Size
  - Transaction counts (all, broker1, broker2)
  - Timestamps (propose, commit)
  - Confirmation latencies

However, the CSV files **do not currently track**:
- Which specific node (NodeID) proposed each block
- Takeover events and their duration
- View change events
- Leadership transitions

## Recommendations

### To Verify Block Generation Discrepancy:

1. **Add NodeID to Block Metrics**
   - Modify `BlockInfoMsg` to include `ProposerNodeID`
   - Track which node in each shard produced each block
   - Analyze per-node block production statistics

2. **Track Leadership Events**
   - Log view changes with timestamps
   - Record leader rotation events
   - Correlate with block production drops

3. **Track Takeover Events** (Being implemented)
   - Record takeover start/end times
   - Track which shards took over for which targets
   - Measure impact on block production during takeover periods

4. **Analyze Existing CSV Data**
   - Count blocks per shard
   - Calculate time gaps between blocks
   - Identify patterns in TxPool sizes
   - Correlate with epoch transitions

### Implementation Status

✅ Analysis completed
🔄 Takeover Duration Module (in progress)
⏳ NodeID tracking enhancement (recommended)
⏳ View change tracking (recommended)

## Conclusion

The discrepancy in block production (100+ vs 30-40 blocks) is primarily caused by:

1. **PBFT Leader Rotation**: Nodes produce blocks only when they are the current leader
2. **Takeover Redistribution**: Healthy nodes take over failed shards' traffic, producing additional blocks
3. **View Changes**: Leadership interruptions affect individual node's block production
4. **Transaction Load Imbalance**: Different shards receive different transaction volumes
5. **Failure Events**: Failed nodes produce fewer blocks during downtime

This is **expected behavior** in a PBFT-based system with reputation supervision and dynamic load balancing. The takeover mechanism is designed to ensure system availability by redirecting traffic from failed shards to healthy ones, naturally causing uneven block distribution.
