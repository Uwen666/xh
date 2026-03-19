// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
	"blockEmulator/utils"
	"bufio"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp control
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	comMod committee.CommitteeModule

	// measure components
	testMeasureMods []measure.MeasureModule
	CurEpochSeed    int64
	CurrentMainMap  map[uint64]uint64
	CurrentBSGMap   map[uint64][]uint64
	CurrentEpoch    int // Track current epoch for takeover events
}

type SupervisionTopology struct {
	MainMap map[uint64]uint64   // Key: Supervisor, Value: Target
	BSGMap  map[uint64][]uint64 // Key: Target, Value: []BSG_Member_ShardID
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, measureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.CurEpochSeed = time.Now().UnixNano()

	d.Ss = signal.NewStopSignal(3 * int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.comMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "CLPA":
		d.comMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize, params.ReconfigTimeGap)
	case "Broker":
		d.comMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	default:
		d.comMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.DatasetFile, params.TotalDataSize, params.TxBatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range measureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		case "Tx_Details":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxDetail())
		case "TakeoverDuration":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TakeoverDuration())
		default:
		}
	}
}

// Supervisor received the block information from the leaders, and handle these
// message to measure the performances.
func (d *Supervisor) handleBlockInfos(content []byte) {
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc()
	} else {
		d.Ss.StopGap_Reset()
	}

	// Update current epoch
	if bim.Epoch > d.CurrentEpoch {
		d.CurrentEpoch = bim.Epoch
	}

	d.comMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	d.comMod.MsgSendingControl()
	epochTicker := 0
	currentEpoch := 0
	// TxHandling is end
	for !d.Ss.GapEnough() { // wait all txs to be handled
		time.Sleep(time.Second)
		if params.ConsensusMethod == params.ConsensusMethod_Reputation {
			epochTicker++
			// ReconfigTimeGap 是以秒为单位的间隔
			if epochTicker >= params.ReconfigTimeGap {
				epochTicker = 0
				currentEpoch++

				// 触发重配置
				go d.EpochReconfiguration(currentEpoch)
			}
		}
	}
	// send stop message
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
		}
	}
	// make sure all stop messages are sent.
	time.Sleep(time.Duration(params.Delay+params.JitterRange+3) * time.Millisecond)

	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	case message.CJoinRequest:
		d.handleJoinRequest(content)
	case message.CUploadTB:
		d.handleUploadTB(content)
	case message.CSupervisionRes:
		d.handleSupervisionRes(content)
	default:
		d.comMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleJoinRequest(content []byte) {
	var msg message.JoinRequestMsg
	json.Unmarshal(content, &msg)

	// 1. 获取系统当前的随机种子

	systemSeed := d.CurEpochSeed

	d.sl.Slog.Printf("Received Join Request from Node %d (Shard %d). Verifying PoW...\n", msg.NodeID, msg.ShardID)

	// 2. 验证 POW
	if utils.VerifyPoW(systemSeed, msg.NodeID, msg.Nonce) {
		d.sl.Slog.Printf("✔ Node %d (Shard %d) passed PoW check. Access Granted.\n", msg.NodeID, msg.ShardID)

		// 3. 发送成功响应
		res := message.JoinResponseMsg{Success: true, Message: "Welcome to the network"}
		resBytes, _ := json.Marshal(res)
		respMsg := message.MergeMessage(message.CJoinResponse, resBytes)

		// 查找该节点的 IP 并发送

		if nodes, ok := d.Ip_nodeTable[msg.ShardID]; ok {
			if targetIP, ok := nodes[msg.NodeID]; ok {
				networks.TcpDial(respMsg, targetIP)
			}
		}

	} else {
		d.sl.Slog.Printf("✘ Node %d failed PoW check. Access Denied.\n", msg.NodeID)
		// 发送失败响应
		res := message.JoinResponseMsg{Success: false, Message: "PoW verification failed"}
		resBytes, _ := json.Marshal(res)
		respMsg := message.MergeMessage(message.CJoinResponse, resBytes)

		if nodes, ok := d.Ip_nodeTable[msg.ShardID]; ok {
			if targetIP, ok := nodes[msg.NodeID]; ok {
				networks.TcpDial(respMsg, targetIP)
			}
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}

// 处理 TB 上传 (L1 验证)
func (d *Supervisor) handleUploadTB(content []byte) {
	var msg message.UploadTBMsg
	json.Unmarshal(content, &msg)

	// 模拟验证签名
	// verifySignature(msg.Signature) ...

	d.sl.Slog.Printf("L1: TB from Shard %d received and verified.\n", msg.ShardID)

	// 发送确认回执
	confirmMsg := message.MergeMessage(message.CConfirmTB, []byte("Verified"))
	// 广播给该分片的所有节点 (包括 CS 和 SS)
	for _, ip := range d.Ip_nodeTable[msg.ShardID] {
		networks.TcpDial(confirmMsg, ip)
	}

	// TODO: 在这里触发声誉更新 (正常工作 +1)
	d.updateShardReputation(msg.ShardID, true)
}

func (d *Supervisor) handleSupervisionRes(content []byte) {
	var msg message.SupervisionResultMsg
	json.Unmarshal(content, &msg)

	d.sl.Slog.Printf("L1: Received Supervision Report from Shard %d against Shard %d. Faulty: %v\n",
		msg.ReporterShardID, msg.TargetShardID, msg.IsFaulty)

	if msg.IsFaulty {
		faultyShardID := msg.TargetShardID
		nTotal := float64(d.ChainConfig.Nodes_perShard)
		nActive := float64(0)
		if nodes, ok := d.Ip_nodeTable[msg.ReporterShardID]; ok {
			nActive = float64(len(nodes))
		}
		if nTotal == 0 {
			nTotal = 1
		}

		cDynamic := float64(params.HealthFactorBase) * (nActive / nTotal)
		if cDynamic < 1.0 {
			cDynamic = 1.0
		}

		poolSum := float64(msg.CurrentPoolSize + msg.TargetPoolSize)
		lNew := poolSum / cDynamic

		d.sl.Slog.Printf("L1: Load Calculation Details:\n")
		d.sl.Slog.Printf("  >> N_active: %.0f, N_total: %.0f\n", nActive, nTotal)
		d.sl.Slog.Printf("  >> C_dynamic: %.2f (Base: %.2f)\n", cDynamic, params.HealthFactorBase)
		d.sl.Slog.Printf("  >> Pool Sum: %.0f (CS_l: %d, CS_m: %d)\n", poolSum, msg.CurrentPoolSize, msg.TargetPoolSize)
		d.sl.Slog.Printf("  >> L_new: %.4f (Threshold: %.2f)\n", lNew, params.TakeoverThreshold)

		// Record takeover start time
		takeoverStartTime := time.Now()

		if lNew <= params.TakeoverThreshold {
			// 允许 CS_l 接管
			d.sl.Slog.Println("L1: Decision -> CS_l Takeover Granted.")

			takeoverMsg := message.MergeMessage(message.CTakeover, []byte("Granted"))
			// 发送给报告者 (CS_l)
			networks.TcpDial(takeoverMsg, msg.ReporterNode.IPaddr)
			d.sl.Slog.Printf("L1: Redirecting traffic from Shard %d to Shard %d\n", msg.TargetShardID, msg.ReporterShardID)
			reporterIPs := d.Ip_nodeTable[msg.ReporterShardID]
			d.Ip_nodeTable[msg.TargetShardID] = reporterIPs

			// Record single takeover event
			d.recordTakeoverEvent("Single", msg.ReporterShardID, []uint64{}, msg.TargetShardID,
				takeoverStartTime, lNew, msg.CurrentPoolSize, msg.TargetPoolSize)
		} else {
			// 负载过高，转交 BSG (Back-up Supervisor Group)
			d.sl.Slog.Println("L1: Decision -> CS_l Overloaded. Activating Back-up Supervisor Group (BSG).")

			// 1. 获取目标分片(CS_m)对应的 BSG 成员列表
			bsgShards, ok := d.CurrentBSGMap[msg.TargetShardID]
			if !ok || len(bsgShards) == 0 {
				d.sl.Slog.Printf("Error: No BSG configuration found for Shard %d\n", msg.TargetShardID)
				return
			}

			// 2. 构造接管指令
			takeoverMsg := message.MergeMessage(message.CTakeover, []byte("BSG_Activated"))

			// 3. 构建新的"虚拟"路由表 (聚合所有 BSG 节点的 IP)
			// 目标：将发往 TargetShard 的交易，均匀分发给 BSG 中的所有节点
			combinedNodeMap := make(map[uint64]string)
			virtualNodeIdx := uint64(0)

			d.sl.Slog.Printf("L1: Redirecting traffic to BSG Shards: %v\n", bsgShards)

			for _, bsgShardID := range bsgShards {
				// 遍历该 BSG 分片内的所有节点
				d.tcpLock.Lock()
				if nodes, exists := d.Ip_nodeTable[bsgShardID]; exists {
					for _, ip := range nodes {
						// A. 发送接管指令给 BSG 成员节点
						// 告诉它们："开始准备接收并处理目标分片的交易"
						networks.TcpDial(takeoverMsg, ip)

						// B. 将该 IP 加入到"虚拟"路由表中
						// 使用递增的 virtualNodeIdx 作为临时 NodeID
						combinedNodeMap[virtualNodeIdx] = ip
						virtualNodeIdx++
					}
				}
				d.tcpLock.Unlock()
			}

			// 4. 执行路由重定向 (Traffic Redirection)
			// 修改 Supervisor 的内存路由表：
			// 此后，comMod 组件向 TargetShardID 发送交易时，实际上会发给 combinedNodeMap 中的节点
			// 也就是发给了整个 BSG 组
			d.Ip_nodeTable[msg.TargetShardID] = combinedNodeMap

			d.sl.Slog.Printf("L1: Traffic redirection complete. %d BSG nodes are now handling Shard %d's transactions.\n",
				len(combinedNodeMap), msg.TargetShardID)

			// Record BSG takeover event
			takeoverShardID := bsgShards[0] // Use first BSG shard as primary takeover shard
			d.recordTakeoverEvent("BSG", takeoverShardID, bsgShards, msg.TargetShardID,
				takeoverStartTime, lNew, msg.CurrentPoolSize, msg.TargetPoolSize)
		}

		d.updateShardReputation(msg.TargetShardID, false)
		if supervisedTarget, ok := d.CurrentMainMap[faultyShardID]; ok {
			csM := supervisedTarget
			d.sl.Slog.Printf("L1: Cascading Alert! Faulty Shard %d was the Supervisor for Shard %d.\n", faultyShardID, csM)

			// 1. 找到 CS_m 的 BSG
			bsgList, ok := d.CurrentBSGMap[csM]
			if ok && len(bsgList) > 0 {
				d.sl.Slog.Printf("L1: Activating BSG %v to supervise Shard %d.\n", bsgList, csM)

				// 2. 发送"接管监督"指令 (CTakeoverSupervision)
				// 通知 BSG 成员: "开始接收并验证 CS_m 的 TB/Block"
				type SupervisionTakeoverInfo struct {
					TargetShardID uint64
				}
				info := SupervisionTakeoverInfo{TargetShardID: csM}
				bytes, _ := json.Marshal(info)
				// 定义新消息类型或复用 CTokeover (建议用新类型以示区分，或者在 CTakeover 中加 Tag)
				// 这里为了简便，假设我们在 message.go 定义了 CTakeoverSupervision
				newMsg := message.MergeMessage(message.CTakeoverSupervision, bytes)
				for _, memberID := range bsgList {
					// 排除掉失效的 CS_l (虽然 kickNodeFromBSGs 应该已经清理过了)
					if memberID == faultyShardID {
						continue
					}

					if nodes, exists := d.Ip_nodeTable[memberID]; exists {
						for _, ip := range nodes {
							networks.TcpDial(newMsg, ip)
						}
					}
				}
			}
		}
	}

}

// recordTakeoverEvent creates and sends a TakeoverEventMsg to measurement modules
func (d *Supervisor) recordTakeoverEvent(takeoverType string, takeoverShardID uint64, bsgShardIDs []uint64,
	targetShardID uint64, startTime time.Time, loadFactor float64, reporterPoolSize int, targetPoolSize int) {

	endTime := time.Now()
	durationMs := endTime.Sub(startTime).Milliseconds()

	takeoverEvent := message.TakeoverEventMsg{
		Epoch:            d.CurrentEpoch,
		TakeoverType:     takeoverType,
		TakeoverShardID:  takeoverShardID,
		BSGShardIDs:      bsgShardIDs,
		TargetShardID:    targetShardID,
		StartTime:        startTime,
		EndTime:          endTime,
		DurationMs:       durationMs,
		LoadFactor:       loadFactor,
		ReporterPoolSize: reporterPoolSize,
		TargetPoolSize:   targetPoolSize,
	}

	// Send to all measurement modules
	eventBytes, _ := json.Marshal(takeoverEvent)
	for _, mod := range d.testMeasureMods {
		mod.HandleExtraMessage(eventBytes)
	}

	d.sl.Slog.Printf("L1: Recorded %s Takeover Event - Target: %d, Takeover: %d, Duration: %dms\n",
		takeoverType, targetShardID, takeoverShardID, durationMs)
}

func (d *Supervisor) updateShardReputation(shardID uint64, success bool) {
	for nodeID, nodeIP := range d.Ip_nodeTable[shardID] {
		// 模拟：在内存中找到节点对象并更新
		// 注意：Supervisor 维护的节点列表可能需要从 d.ChainConfig 或其他地方获取
		// 这里简化为打印日志，实际需维护一个 map[string]*shard.Node 的全网节点表
		status := "Penalty"
		if success {
			status = "Reward"
		}
		d.sl.Slog.Printf("Reputation Update for Node %d (IP: %s) in Shard %d: %s\n", nodeID, nodeIP, shardID, status)

	}
}

func (d *Supervisor) generateSupervisionTopology(epoch int, randomSeed int64) SupervisionTopology {
	shardNum := int(d.ChainConfig.ShardNums)
	shards := make([]uint64, shardNum)
	for i := 0; i < shardNum; i++ {
		shards[i] = uint64(i)
	}

	// 1. Shuffle (随机洗牌)
	r := rand.New(rand.NewSource(randomSeed))
	r.Shuffle(shardNum, func(i, j int) {
		shards[i], shards[j] = shards[j], shards[i]
	})

	mainMap := make(map[uint64]uint64)
	bsgMap := make(map[uint64][]uint64)

	// 辅助映射：快速查找每个分片的主监督者
	targetToMain := make(map[uint64]uint64)

	// 2. 构建主监督关系 (环形: Shard[i] -> Shard[i+1])
	for i := 0; i < shardNum; i++ {
		supervisor := shards[i]
		target := shards[(i+1)%shardNum]

		mainMap[supervisor] = target
		targetToMain[target] = supervisor

		d.sl.Slog.Printf("[Epoch %d] Main Relation: Shard %d -> Shard %d\n", epoch, supervisor, target)
	}

	const BSGSize = 2

	for target, mainSupervisor := range targetToMain { // target 即 CS_f
		bsg := make([]uint64, 0)

		// (A) 添加强制成员: CS_l 的主监督 (Grandparent)
		// 如果 A->B->C (C是target), 则 main=B, grand=A
		grandSupervisor := targetToMain[mainSupervisor]
		bsg = append(bsg, grandSupervisor)

		// (B) 填充剩余成员 (随机且互不相关)
		// 候选池: 所有分片 - {Target, Main, Grand}
		candidates := make([]uint64, 0)
		for _, s := range shards {
			if s != target && s != mainSupervisor && s != grandSupervisor {
				candidates = append(candidates, s)
			}
		}

		// 打乱候选池
		r.Shuffle(len(candidates), func(i, j int) {
			candidates[i], candidates[j] = candidates[j], candidates[i]
		})

		// 填充满 BSGSize
		needed := BSGSize - 1 // 减去已经加入的 Grandparent
		for k := 0; k < needed && k < len(candidates); k++ {
			bsg = append(bsg, candidates[k])
		}

		bsgMap[target] = bsg
		d.sl.Slog.Printf("[Epoch %d] BSG for Shard %d: %v (Grandparent: %d)\n",
			epoch, target, bsg, grandSupervisor)
	}

	return SupervisionTopology{
		MainMap: mainMap,
		BSGMap:  bsgMap,
	}
}

// 执行 Epoch 重配置
func (d *Supervisor) EpochReconfiguration(epoch int) {
	d.sl.Slog.Printf("=== Starting Sharding Reconfiguration for Epoch %d ===\n", epoch)

	// 1. 收集所有活跃节点 (IP)
	// 在实际方案中，这里应剔除声誉低于阈值的节点
	// 这里简化为：收集当前 Ip_nodeTable 中的所有节点 IP
	// 1. 分类收集节点 (CS vs SS)
	// csNodesIPs: 收集所有共识节点的 IP，准备打乱
	csNodesIPs := make([]string, 0)
	// ssNodesIPs: 记录存储节点的 IP，保持位置不变 [ShardID][NodeID] -> IP
	ssNodesIPs := make(map[uint64]map[uint64]string)

	// 假设 NodeID < 3 是共识节点 (Consensus)，其余是存储节点 (Storage)
	// 这是一个基于约定的逻辑，实际项目中可配置参数
	const ConsensusNodeLimit = 3

	for sid, nodes := range d.Ip_nodeTable {
		if _, ok := ssNodesIPs[sid]; !ok {
			ssNodesIPs[sid] = make(map[uint64]string)
		}
		for nid, ip := range nodes {
			if nid < ConsensusNodeLimit {
				// 共识节点 -> 加入洗牌池
				csNodesIPs = append(csNodesIPs, ip)
			} else {
				// 存储节点 -> 原地保留
				ssNodesIPs[sid][nid] = ip
			}
		}
	}

	epochRandomness := time.Now().UnixNano()
	r := rand.New(rand.NewSource(epochRandomness))
	r.Shuffle(len(csNodesIPs), func(i, j int) {
		csNodesIPs[i], csNodesIPs[j] = csNodesIPs[j], csNodesIPs[i]
	})
	d.tcpLock.Lock()
	d.CurEpochSeed = epochRandomness
	d.tcpLock.Unlock()

	// 3. 重新构建分片结构 (New IP Table)
	newIpNodeTable := make(map[uint64]map[uint64]string)

	// 辅助 Map: 记录每个 IP 的新身份 (IP -> {ShardID, NodeID})
	type NodeIdentity struct {
		ShardID uint64
		NodeID  uint64
	}
	ipToIdentity := make(map[string]NodeIdentity)

	csIdx := 0
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		newIpNodeTable[sid] = make(map[uint64]string)
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			var ip string

			if nid < ConsensusNodeLimit {
				// 填入打乱后的共识节点
				if csIdx < len(csNodesIPs) {
					ip = csNodesIPs[csIdx]
					csIdx++
				}
			} else {
				// 填入原来的存储节点
				ip = ssNodesIPs[sid][nid]
			}

			if ip != "" {
				newIpNodeTable[sid][nid] = ip
				ipToIdentity[ip] = NodeIdentity{ShardID: sid, NodeID: nid}
			}
		}
	}

	// 更新 Supervisor 本地的表 (注意并发安全，最好加锁，这里简化)
	d.Ip_nodeTable = newIpNodeTable

	// 4. 生成新的监督拓扑 (基于新的分片结构)
	topology := d.generateSupervisionTopology(epoch, epochRandomness)

	// 预处理 BSG 反向索引
	nodeToBackupTargets := make(map[uint64][]uint64)
	for target, members := range topology.BSGMap {
		for _, member := range members {
			nodeToBackupTargets[member] = append(nodeToBackupTargets[member], target)
		}
	}
	d.CurrentBSGMap = topology.BSGMap
	d.CurrentMainMap = topology.MainMap
	// 5. 广播重配置消息给所有节点
	for ip, identity := range ipToIdentity {
		// 获取该节点的新监督任务
		mainTarget := topology.MainMap[identity.ShardID]
		// 注意：BSG 是基于分片的，该分片的所有节点都承担相同的 BSG 任务
		backupTargets := nodeToBackupTargets[identity.ShardID]

		info := message.ReconfigInfo{
			Epoch:         epoch,
			NewShardID:    identity.ShardID,
			NewNodeID:     identity.NodeID,
			NewIPTable:    newIpNodeTable, // 下发新的全网表
			MainTarget:    mainTarget,
			BackupTargets: backupTargets,
			FullBSGMap:    topology.BSGMap,
		}

		bytes, _ := json.Marshal(info)
		msg := message.MergeMessage(message.CReconfig, bytes)

		// 发送给节点
		networks.TcpDial(msg, ip)

		d.sl.Slog.Printf("Reconfig sent to %s: Now Shard %d, Node %d\n", ip, identity.ShardID, identity.NodeID)
	}
}
