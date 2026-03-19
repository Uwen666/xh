package measure

import (
	"blockEmulator/message"
	"encoding/json"
	"strconv"
)

// TestModule_TakeoverDuration tracks takeover events and their durations per epoch
type TestModule_TakeoverDuration struct {
	epochID int

	// Per-epoch tracking
	takeoverCounts      []int                          // Number of takeovers per epoch
	totalDurationMs     []int64                        // Total takeover duration (ms) per epoch
	singleTakeoverCount []int                          // Single shard takeovers per epoch
	bsgTakeoverCount    []int                          // BSG takeovers per epoch
	avgLoadFactor       []float64                      // Average load factor per epoch

	// Detailed event records for CSV output
	takeoverEvents []message.TakeoverEventMsg

	// Active takeover tracking (for calculating end time)
	activeTakeovers map[uint64]*message.TakeoverEventMsg // Key: TargetShardID
}

func NewTestModule_TakeoverDuration() *TestModule_TakeoverDuration {
	return &TestModule_TakeoverDuration{
		epochID:             -1,
		takeoverCounts:      make([]int, 0),
		totalDurationMs:     make([]int64, 0),
		singleTakeoverCount: make([]int, 0),
		bsgTakeoverCount:    make([]int, 0),
		avgLoadFactor:       make([]float64, 0),
		takeoverEvents:      make([]message.TakeoverEventMsg, 0),
		activeTakeovers:     make(map[uint64]*message.TakeoverEventMsg),
	}
}

func (ttd *TestModule_TakeoverDuration) OutputMetricName() string {
	return "Takeover_Duration_Module_ms"
}

// UpdateMeasureRecord handles BlockInfoMsg (no-op for this module)
// Takeover tracking is done via HandleExtraMessage
func (ttd *TestModule_TakeoverDuration) UpdateMeasureRecord(b *message.BlockInfoMsg) {
	// Update epoch tracking based on block messages
	if b.BlockBodyLength == 0 {
		return
	}

	epochid := b.Epoch

	// Extend arrays if we see a new epoch
	for ttd.epochID < epochid {
		ttd.takeoverCounts = append(ttd.takeoverCounts, 0)
		ttd.totalDurationMs = append(ttd.totalDurationMs, 0)
		ttd.singleTakeoverCount = append(ttd.singleTakeoverCount, 0)
		ttd.bsgTakeoverCount = append(ttd.bsgTakeoverCount, 0)
		ttd.avgLoadFactor = append(ttd.avgLoadFactor, 0)
		ttd.epochID++
	}
}

// HandleExtraMessage processes TakeoverEventMsg messages
func (ttd *TestModule_TakeoverDuration) HandleExtraMessage(msg []byte) {
	var takeoverEvent message.TakeoverEventMsg
	if err := json.Unmarshal(msg, &takeoverEvent); err != nil {
		// Not a takeover event message, ignore
		return
	}

	epochid := takeoverEvent.Epoch

	// Extend arrays if we see a new epoch
	for ttd.epochID < epochid {
		ttd.takeoverCounts = append(ttd.takeoverCounts, 0)
		ttd.totalDurationMs = append(ttd.totalDurationMs, 0)
		ttd.singleTakeoverCount = append(ttd.singleTakeoverCount, 0)
		ttd.bsgTakeoverCount = append(ttd.bsgTakeoverCount, 0)
		ttd.avgLoadFactor = append(ttd.avgLoadFactor, 0)
		ttd.epochID++
	}

	// Store the complete event
	ttd.takeoverEvents = append(ttd.takeoverEvents, takeoverEvent)

	// Update per-epoch statistics
	ttd.takeoverCounts[epochid]++
	ttd.totalDurationMs[epochid] += takeoverEvent.DurationMs

	if takeoverEvent.TakeoverType == "Single" {
		ttd.singleTakeoverCount[epochid]++
	} else if takeoverEvent.TakeoverType == "BSG" {
		ttd.bsgTakeoverCount[epochid]++
	}

	// Update average load factor (running average)
	currentAvg := ttd.avgLoadFactor[epochid]
	count := ttd.takeoverCounts[epochid]
	ttd.avgLoadFactor[epochid] = (currentAvg*float64(count-1) + takeoverEvent.LoadFactor) / float64(count)
}

// OutputRecord returns per-epoch takeover statistics
func (ttd *TestModule_TakeoverDuration) OutputRecord() (perEpochAvgDuration []float64, totalAvgDuration float64) {
	// Write detailed CSV
	ttd.writeToCSV()

	// Calculate simple result
	perEpochAvgDuration = make([]float64, ttd.epochID+1)
	totalDuration := int64(0)
	totalCount := 0

	for eid, count := range ttd.takeoverCounts {
		if count > 0 {
			perEpochAvgDuration[eid] = float64(ttd.totalDurationMs[eid]) / float64(count)
		} else {
			perEpochAvgDuration[eid] = 0
		}
		totalDuration += ttd.totalDurationMs[eid]
		totalCount += count
	}

	if totalCount > 0 {
		totalAvgDuration = float64(totalDuration) / float64(totalCount)
	} else {
		totalAvgDuration = 0
	}

	return
}

func (ttd *TestModule_TakeoverDuration) writeToCSV() {
	// Write per-epoch summary
	ttd.writeEpochSummary()

	// Write detailed event log
	ttd.writeEventDetails()
}

// writeEpochSummary writes per-epoch aggregated statistics
func (ttd *TestModule_TakeoverDuration) writeEpochSummary() {
	fileName := ttd.OutputMetricName()
	measureName := []string{
		"EpochID",
		"Total Takeover Count",
		"Single Takeover Count",
		"BSG Takeover Count",
		"Total Duration (ms)",
		"Avg Duration per Takeover (ms)",
		"Avg Load Factor at Takeover",
	}
	measureVals := make([][]string, 0)

	for eid := 0; eid <= ttd.epochID; eid++ {
		avgDuration := float64(0)
		if ttd.takeoverCounts[eid] > 0 {
			avgDuration = float64(ttd.totalDurationMs[eid]) / float64(ttd.takeoverCounts[eid])
		}

		csvLine := []string{
			strconv.Itoa(eid),
			strconv.Itoa(ttd.takeoverCounts[eid]),
			strconv.Itoa(ttd.singleTakeoverCount[eid]),
			strconv.Itoa(ttd.bsgTakeoverCount[eid]),
			strconv.FormatInt(ttd.totalDurationMs[eid], 10),
			strconv.FormatFloat(avgDuration, 'f', 2, 64),
			strconv.FormatFloat(ttd.avgLoadFactor[eid], 'f', 4, 64),
		}
		measureVals = append(measureVals, csvLine)
	}

	WriteMetricsToCSV(fileName, measureName, measureVals)
}

// writeEventDetails writes detailed per-event records
func (ttd *TestModule_TakeoverDuration) writeEventDetails() {
	fileName := ttd.OutputMetricName() + "_Details"
	measureName := []string{
		"EpochID",
		"Takeover Type",
		"Takeover Shard ID",
		"Target Shard ID",
		"BSG Shard IDs",
		"Start Time (UnixMilli)",
		"End Time (UnixMilli)",
		"Duration (ms)",
		"Load Factor (L_new)",
		"Reporter Pool Size",
		"Target Pool Size",
		"Total Redirected Txs",
	}
	measureVals := make([][]string, 0)

	for _, event := range ttd.takeoverEvents {
		bsgShardIDsStr := ""
		if len(event.BSGShardIDs) > 0 {
			bsgShardIDsStr = "["
			for i, sid := range event.BSGShardIDs {
				if i > 0 {
					bsgShardIDsStr += ","
				}
				bsgShardIDsStr += strconv.FormatUint(sid, 10)
			}
			bsgShardIDsStr += "]"
		}

		csvLine := []string{
			strconv.Itoa(event.Epoch),
			event.TakeoverType,
			strconv.FormatUint(event.TakeoverShardID, 10),
			strconv.FormatUint(event.TargetShardID, 10),
			bsgShardIDsStr,
			strconv.FormatInt(event.StartTime.UnixMilli(), 10),
			strconv.FormatInt(event.EndTime.UnixMilli(), 10),
			strconv.FormatInt(event.DurationMs, 10),
			strconv.FormatFloat(event.LoadFactor, 'f', 4, 64),
			strconv.Itoa(event.ReporterPoolSize),
			strconv.Itoa(event.TargetPoolSize),
			strconv.Itoa(event.TotalRedirectedTxs),
		}
		measureVals = append(measureVals, csvLine)
	}

	WriteMetricsToCSV(fileName, measureName, measureVals)
}
