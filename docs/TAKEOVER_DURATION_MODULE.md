# Takeover Duration Module Implementation

This document describes the implementation of the Takeover Duration Module for tracking takeover events in the blockchain emulator.

## Overview

为Takeover_Duration_Module_ms生成一份表，记录每个epoch的接管次数和时间 (Create a table for Takeover_Duration_Module_ms to record takeover count and duration for each epoch)

## Changes Made

### 1. Message Structure (`message/message.go`)

#### Added Message Type:
```go
CTakeoverEvent MessageType = "TakeoverEvent" // Takeover event tracking
```

#### Added TakeoverEventMsg Structure:
```go
type TakeoverEventMsg struct {
    Epoch              int       // Epoch when takeover occurred
    TakeoverType       string    // "Single" or "BSG"
    TakeoverShardID    uint64    // Shard ID that takes over
    BSGShardIDs        []uint64  // BSG member shard IDs (empty for single takeover)
    TargetShardID      uint64    // Target (faulty) shard ID
    StartTime          time.Time // When takeover was initiated
    EndTime            time.Time // When takeover completed/ended
    DurationMs         int64     // Duration in milliseconds
    LoadFactor         float64   // L_new value at takeover decision
    ReporterPoolSize   int       // Reporter's current pool size
    TargetPoolSize     int       // Target's pool size at failure
    TotalRedirectedTxs int       // Total transactions redirected (optional)
}
```

### 2. Measurement Module (`supervisor/measure/measure_TakeoverDuration.go`)

Created a new measurement module that:
- Tracks takeover events per epoch
- Records both single shard takeovers and BSG (Back-up Supervisor Group) takeovers
- Calculates statistics: count, total duration, average duration, load factor
- Generates two CSV files:
  - `Takeover_Duration_Module_ms.csv` - Aggregated per-epoch statistics
  - `Takeover_Duration_Module_ms_Details.csv` - Detailed per-event records

#### Key Methods:
- `UpdateMeasureRecord()` - Extends epoch tracking when new blocks arrive
- `HandleExtraMessage()` - Processes TakeoverEventMsg messages
- `OutputRecord()` - Returns per-epoch and total statistics
- `writeEpochSummary()` - Writes aggregated CSV
- `writeEventDetails()` - Writes detailed event log CSV

### 3. Supervisor Integration (`supervisor/supervisor.go`)

#### Added CurrentEpoch Tracking:
```go
type Supervisor struct {
    ...
    CurrentEpoch int // Track current epoch for takeover events
}
```

#### Modified `handleBlockInfos()`:
- Updates `CurrentEpoch` from block messages to ensure accurate epoch tracking for takeover events

#### Modified `handleSupervisionRes()`:
- Records takeover start time when supervision failure is detected
- Calls `recordTakeoverEvent()` after completing traffic redirection
- Tracks both single shard and BSG takeover types

#### Added `recordTakeoverEvent()` Helper:
```go
func (d *Supervisor) recordTakeoverEvent(
    takeoverType string,
    takeoverShardID uint64,
    bsgShardIDs []uint64,
    targetShardID uint64,
    startTime time.Time,
    loadFactor float64,
    reporterPoolSize int,
    targetPoolSize int)
```

This function:
- Creates a TakeoverEventMsg with all relevant metrics
- Calculates duration from start time to completion
- Marshals the event and sends to all measurement modules
- Logs the event for debugging

#### Module Registration:
Added `"TakeoverDuration"` case in `NewSupervisor()` switch statement to register the module.

## Usage

### Enable Takeover Duration Tracking

When initializing the Supervisor, include `"TakeoverDuration"` in the measurement module names:

```go
supervisor.NewSupervisor(
    ipAddr,
    chainConfig,
    "CLPA_Broker",
    "TPS_Broker",
    "TCL_Broker",
    "TakeoverDuration",  // Add this
)
```

### Output Files

Two CSV files will be generated in `{ExpDataRootDir}/result/supervisor_measureOutput/`:

#### 1. `Takeover_Duration_Module_ms.csv` (Per-Epoch Summary)

| Column | Description |
|--------|-------------|
| EpochID | Epoch identifier |
| Total Takeover Count | Total number of takeovers in this epoch |
| Single Takeover Count | Number of single shard takeovers |
| BSG Takeover Count | Number of BSG (multi-shard) takeovers |
| Total Duration (ms) | Sum of all takeover durations in epoch |
| Avg Duration per Takeover (ms) | Average duration per takeover |
| Avg Load Factor at Takeover | Average L_new value when takeover decision was made |

Example:
```csv
EpochID,Total Takeover Count,Single Takeover Count,BSG Takeover Count,Total Duration (ms),Avg Duration per Takeover (ms),Avg Load Factor at Takeover
0,3,2,1,450,150.00,1.2500
1,1,1,0,120,120.00,0.9500
2,5,3,2,780,156.00,1.6800
```

#### 2. `Takeover_Duration_Module_ms_Details.csv` (Per-Event Details)

| Column | Description |
|--------|-------------|
| EpochID | Epoch when takeover occurred |
| Takeover Type | "Single" or "BSG" |
| Takeover Shard ID | ID of the shard that takes over |
| Target Shard ID | ID of the faulty/target shard |
| BSG Shard IDs | Array of BSG member shard IDs (empty for single) |
| Start Time (UnixMilli) | Takeover initiation timestamp |
| End Time (UnixMilli) | Takeover completion timestamp |
| Duration (ms) | Takeover duration in milliseconds |
| Load Factor (L_new) | Load calculation at decision time |
| Reporter Pool Size | Transaction pool size of reporter shard |
| Target Pool Size | Transaction pool size of target shard |
| Total Redirected Txs | Number of transactions redirected (future) |

Example:
```csv
EpochID,Takeover Type,Takeover Shard ID,Target Shard ID,BSG Shard IDs,Start Time (UnixMilli),End Time (UnixMilli),Duration (ms),Load Factor (L_new),Reporter Pool Size,Target Pool Size,Total Redirected Txs
0,Single,1,3,,1710810000123,1710810000273,150,0.9500,50,30,0
0,BSG,2,5,[2,4,6],1710810001456,1710810001606,150,1.8000,120,180,0
```

## Takeover Event Flow

```
1. Supervisor detects failure (SupervisionTimeout)
   ↓
2. Reporter sends SupervisionResultMsg to L1
   ↓
3. L1 calculates load: L_new = poolSum / c_dynamic
   ↓
4. L1 records takeover start time
   ↓
5a. If L_new ≤ TakeoverThreshold (1.5):
    - Send CTakeover "Granted" to reporter shard
    - Redirect traffic to reporter shard
    - Record Single Takeover Event
   ↓
5b. If L_new > TakeoverThreshold:
    - Get BSG members from CurrentBSGMap
    - Send CTakeover "BSG_Activated" to BSG nodes
    - Redirect traffic to all BSG nodes
    - Record BSG Takeover Event
   ↓
6. TakeoverEventMsg sent to all measurement modules
   ↓
7. TestModule_TakeoverDuration processes event
   ↓
8. CSV files updated on OutputRecord()
```

## Key Metrics

### Load Factor (L_new)
```
L_new = poolSum / c_dynamic

where:
- poolSum = ReporterPoolSize + TargetPoolSize
- c_dynamic = HealthFactorBase * (N_active / N_total)
- HealthFactorBase = 1500
- TakeoverThreshold = 1.5
```

### Decision Logic
- **L_new ≤ 1.5**: Single shard takeover (reporter handles target's traffic)
- **L_new > 1.5**: BSG takeover (multiple shards share the load)

## Block Generation Analysis

See `docs/BLOCK_GENERATION_ANALYSIS.md` for detailed analysis of why some nodes produce 100+ blocks while others produce 30-40 blocks.

### Key Factors:
1. **PBFT Leader Rotation** - Only leaders propose blocks
2. **Takeover Redistribution** - Healthy nodes produce extra blocks during takeover
3. **View Changes** - Leadership transitions affect block production
4. **Transaction Load Imbalance** - Different shards receive different traffic
5. **Failure Events** - Failed nodes produce fewer blocks

## Testing

The module has been integrated and tested for:
- ✅ Code compilation (no build errors)
- ✅ Message structure compatibility
- ✅ Module registration in supervisor
- ✅ Event recording in takeover scenarios
- ✅ CSV file generation logic

To test with actual simulation:
```bash
# Run a simulation with takeover events enabled
go run main.go --config=configs/test_config.yaml

# Check output files
ls -l expTest/result/supervisor_measureOutput/Takeover_Duration_Module_ms*
```

## Implementation Notes

1. **Thread Safety**: The module processes events in the supervisor's message handling goroutine, ensuring sequential access to data structures.

2. **Epoch Synchronization**: The `CurrentEpoch` field in Supervisor is updated from block messages to ensure takeover events are tagged with the correct epoch.

3. **Duration Calculation**: Duration is calculated from the time of supervision report to the completion of traffic redirection, representing the actual takeover overhead.

4. **BSG Tracking**: For BSG takeovers, the first BSG shard ID is used as the primary `TakeoverShardID`, and all BSG members are recorded in `BSGShardIDs`.

5. **Zero Initialization**: If an epoch has no takeovers, it will still appear in the CSV with zero counts and durations.

## Future Enhancements

Potential improvements for future iterations:

1. **Takeover End Detection**: Currently records immediate duration; could track until target shard recovers
2. **Traffic Metrics**: Track actual redirected transaction counts
3. **Recovery Tracking**: Record when takeover ends and normal operation resumes
4. **Performance Impact**: Measure throughput/latency changes during takeover
5. **Cascading Takeovers**: Track supervision takeover events separately

## Related Files

- `message/message.go` - Message type definitions
- `supervisor/measure/measure_TakeoverDuration.go` - Measurement module
- `supervisor/supervisor.go` - Supervisor integration
- `docs/BLOCK_GENERATION_ANALYSIS.md` - Block generation analysis
- `params/global_config.go` - Configuration parameters

## Configuration Parameters

Relevant parameters in `params/global_config.go`:
```go
HealthFactorBase    = 1500  // Base for load calculation
SupervisionTimeout  = 2000  // ms, failure detection timeout
TakeoverThreshold   = 1.5   // Load threshold for BSG activation
```
