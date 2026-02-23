package pbft_all

import (
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/utils"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type ReputationSupervisionMod struct {
	pbftNode *PbftConsensusNode

	stateWaitChans map[uint64]chan *message.ReturnStateMsg
	stateLock      sync.Mutex

	monitorTimer  *time.Timer
	superviseLock sync.Mutex

	tbMonitorTimer              *time.Timer
	isTakingOver                bool
	takeoverTarget              uint64
	emergencySupervisingTargets map[uint64]bool
	voteBox                     map[uint64][]*message.BSGVoteMsg
	voteLock                    sync.Mutex
	decisionMade                map[uint64]bool
	fullBSGMap                  map[uint64][]uint64
	mapLock                     sync.RWMutex
	tbSigMap                    map[uint64]map[uint64][]byte
	tbSigLock                   sync.Mutex
	shardRepTable               map[uint64]float64
}

type NodeRep struct {
	NodeID uint64
	Rep    float64
}

func NewReputationSupervisionMod(p *PbftConsensusNode) *ReputationSupervisionMod {
	return &ReputationSupervisionMod{
		pbftNode:                    p,
		stateWaitChans:              make(map[uint64]chan *message.ReturnStateMsg),
		emergencySupervisingTargets: make(map[uint64]bool),
		fullBSGMap:                  make(map[uint64][]uint64),
		tbSigMap:                    make(map[uint64]map[uint64][]byte),
		shardRepTable:               make(map[uint64]float64),
	}
}

func (r *ReputationSupervisionMod) broadcastToShard(targetShardId uint64, msg []byte) {
	nodes, ok := r.pbftNode.ip_nodeTable[targetShardId]
	if !ok {
		fmt.Println("Error: Shard %d not found In IP table", targetShardId)
		return
	}
	for _, ip := range nodes {
		networks.TcpDial(msg, ip)
	}
}

func (r *ReputationSupervisionMod) IsPrimary() bool {
	// view 是 atomic.Int32
	view := uint64(r.pbftNode.view.Load())
	return view%r.pbftNode.node_nums == r.pbftNode.NodeID
}

func (r *ReputationSupervisionMod) HandlePrePrepare(ppMsg *message.PrePrepare) {
	if r.pbftNode.RunningNode.NodeType == shard.NodeType_Storage {
		return
	}
	block := core.DecodeB(ppMsg.RequestMsg.Msg.Content)
	fmt.Printf("[CS %d] Received PrePrepare for Block %d\n", r.pbftNode.ShardID, block.Header.Number)

	accounts := make([]string, 0)
	for _, tx := range block.Body {
		accounts = append(accounts, tx.Sender, tx.Recipient)
	}
	replyChan := make(chan *message.ReturnStateMsg, 1)
	r.stateLock.Lock()
	r.stateWaitChans[block.Header.Number] = replyChan
	r.stateLock.Unlock()

	getStateReq := message.GetStateMsg{
		BlockHeight:   block.Header.Number,
		AccountList:   accounts,
		SenderNode:    r.pbftNode.NodeID,
		SenderShardID: r.pbftNode.ShardID,
	}
	reqBytes, _ := json.Marshal(&getStateReq)
	msg := message.MergeMessage(message.CGetState, reqBytes)

	r.broadcastToShard(r.pbftNode.ShardID, msg)
	if r.isTakingOver {
		fmt.Printf("[CS %d] Takeover: Requesting state from Target Shard %d\n", r.pbftNode.ShardID, r.takeoverTarget)
		r.broadcastToShard(r.takeoverTarget, msg)

	}
	select {
	case stateMsg := <-replyChan:
		fmt.Printf("[CS %d] Received State from SS for Block %d\n", r.pbftNode.ShardID, block.Header.Number, len(stateMsg.AccountList))
	case <-time.After(time.Duration(params.SupervisionTimeout) * time.Millisecond):
		fmt.Printf("[CS %d] Timeout waiting for SS state!\n", r.pbftNode.ShardID)
		return
	}
	if r.isTakingOver {
		fmt.Printf("[CS %d] Processing Takover Txs Shard %d\n", r.pbftNode.ShardID, r.takeoverTarget)
	}
}

func (r *ReputationSupervisionMod) HandlePrepare(pMsg *message.Prepare) {}
func (r *ReputationSupervisionMod) HandleCommit(cMsg *message.Commit) {

	if r.pbftNode.RunningNode.NodeType == shard.NodeType_Consensus {

		// 1. 解析区块获取详细信息 (用于构建 TB)
		digestStr := hex.EncodeToString(cMsg.Digest)
		req, ok := r.pbftNode.requestPool[digestStr]
		if !ok {
			// 如果本地还没有收到 PrePrepare，可能无法处理，直接返回
			return
		}
		block := core.DecodeB(req.Msg.Content)
		blockHash := block.Header.Hash()
		tb := message.TimeBeacon{
			ShardID:     r.pbftNode.ShardID,
			BlockHeight: block.Header.Number,
			BlockHash:   blockHash,                // 区块哈希
			StateRoot:   block.Header.StateRoot,   // 状态树根
			TxRoot:      block.Header.TxRoot,      // 交易树根
			ReceiptRoot: block.Header.ReceiptRoot, // 收据树根
			Timestamp:   time.Now().Unix(),
		}

		// 2. 检查是否有资格签名 (Top 10% + VRF)
		if r.checkSigningEligibility(tb.BlockHeight) {

			// 3. 对 TB 结构体进行签名 (序列化后 Hash 再 Sign)
			tbBytes, _ := json.Marshal(tb)
			sig := r.signData(tbBytes) // 模拟签名函数

			if r.IsPrimary() {
				// Leader 自己存
				r.handleTBSignInternal(r.pbftNode.NodeID, tb, sig)
			} else {
				// Replica 发送给 Leader
				signMsg := message.TBSignMsg{
					TB:        tb,
					NodeID:    r.pbftNode.NodeID,
					Signature: sig,
					VRFProof:  []byte("proof"),
				}
				bytes, _ := json.Marshal(signMsg)
				msg := message.MergeMessage(message.CTBSign, bytes)

				// 发送给 Leader
				currentView := uint64(r.pbftNode.view.Load())

				shardNodeCount := uint64(len(r.pbftNode.ip_nodeTable[r.pbftNode.ShardID]))

				if shardNodeCount > 0 {
					leaderNodeID := currentView % shardNodeCount
					if leaderIP, ok := r.pbftNode.ip_nodeTable[r.pbftNode.ShardID][leaderNodeID]; ok {
						networks.TcpDial(msg, leaderIP)
					}
				}
			}
		}
	}
}

func (r *ReputationSupervisionMod) handleTBSign(content []byte) {
	var msg message.TBSignMsg
	json.Unmarshal(content, &msg)

	// 1. 验证资格 (Leader 复核 VRF)
	// 在真实 VRF 中，这里需要使用 msg.VRFProof 和公钥进行验证
	// 在我们的模拟实现(utils.VRFVerify)中，输入是公开的(NodeID+Height)，
	// 所以 Leader 可以直接重算一遍来验证发送者是否撒谎
	const SelectionThreshold = 0.3
	if !utils.VRFVerify(msg.NodeID, int64(msg.TB.BlockHeight), SelectionThreshold) {
		fmt.Printf("[Leader]  Node %d sent signature but failed VRF check.\n", msg.NodeID)
		return
	}

	// 2. 资格验证通过，存入签名集合
	r.handleTBSignInternal(msg.NodeID, msg.TB, msg.Signature)
}

func (r *ReputationSupervisionMod) handleTBSignInternal(nodeID uint64, tb message.TimeBeacon, sig []byte) {
	height := tb.BlockHeight
	r.tbSigLock.Lock()
	defer r.tbSigLock.Unlock()

	if _, ok := r.tbSigMap[height]; !ok {
		r.tbSigMap[height] = make(map[uint64][]byte)
	}
	r.tbSigMap[height][nodeID] = sig

	// 3. 检查聚合阈值
	// 阈值应动态计算：例如 Top 10% 节点数的 2/3
	// 假设 Top10% 有 10 个，VRF 选一半=5个，那么收到 3~4 个即可提交
	const MinSignaturesRequired = 2

	if len(r.tbSigMap[height]) == MinSignaturesRequired {
		fmt.Printf("[Leader] ⚡ TB Aggregated (Height %d). Uploading...\n", height)

		uploadMsg := message.UploadTBMsg{
			TB:         tb,
			Signatures: r.tbSigMap[height],
		}
		bytes, _ := json.Marshal(uploadMsg)
		sendMsg := message.MergeMessage(message.CUploadTB, bytes)
		networks.TcpDial(sendMsg, params.SupervisorAddr)

		// 清理防止内存泄漏
		delete(r.tbSigMap, height)
	}
}

func (r *ReputationSupervisionMod) signData(data []byte) []byte {
	// 实际应使用 r.pbftNode.RunningNode.PrivateKey 进行签名
	h := sha256.Sum256(data)
	return h[:]
}

func (r *ReputationSupervisionMod) HandleViewChange(vcMsg *message.ViewChangeMsg) {}
func (r *ReputationSupervisionMod) HandleNewView(viewMsg *message.NewViewMsg)     {}

func (r *ReputationSupervisionMod) HandleMessageOutsidePBFT(msgType message.MessageType, content []byte) {
	switch msgType {
	case message.CGetState:
		// SS 处理请求
		if r.pbftNode.RunningNode.NodeType != shard.NodeType_Storage {
			// 如果我是 CS_l，我可能也收到了这个广播（作为监听）
			r.checkSupervisionTrigger(content)
			return
		}

		var req message.GetStateMsg
		json.Unmarshal(content, &req)

		// SS 模拟返回状态
		res := message.ReturnStateMsg{
			BlockHeight: req.BlockHeight,
			StateRoot:   []byte("mock_root"),
		}
		resBytes, _ := json.Marshal(res)
		sendMsg := message.MergeMessage(message.CReturnState, resBytes)

		targetIP := r.pbftNode.ip_nodeTable[req.SenderShardID][req.SenderNode]
		networks.TcpDial(sendMsg, targetIP)

	case message.CReturnState:
		// CS 处理返回
		var res message.ReturnStateMsg
		json.Unmarshal(content, &res)
		r.stateLock.Lock()
		if ch, ok := r.stateWaitChans[res.BlockHeight]; ok {
			ch <- &res
			delete(r.stateWaitChans, res.BlockHeight)
		}
		r.stateLock.Unlock()

	case message.CTakeoverSupervision:
		type SupervisionTakeoverInfo struct {
			TargetShardID uint64
		}
		var info SupervisionTakeoverInfo
		json.Unmarshal(content, &info)

		fmt.Printf("[BSG Node %d] ALERT: Taking over SUPERVISION task for Shard %d\n", r.pbftNode.NodeID, info.TargetShardID)

		r.superviseLock.Lock()
		r.emergencySupervisingTargets[info.TargetShardID] = true
		r.superviseLock.Unlock()

	case message.CBlockInfo:
		var bim message.BlockInfoMsg
		json.Unmarshal(content, &bim)

		isMainTarget := (bim.SenderShardID == r.pbftNode.RunningNode.SuperviseTargetShardID)
		isEmergencyTarget := r.emergencySupervisingTargets[bim.SenderShardID]
		if isMainTarget || isEmergencyTarget {
			if r.IsPrimary() {
				// 启动超时监测 (复用之前的逻辑，或者为每个 Target 维护独立的 Timer)
				// 注意：如果有多个目标，r.monitorTimer 需要改为 map[uint64]*Timer
				r.startBlockTimer(bim.SenderShardID)
			}
		}

	case message.CConfirmTB:
		r.handleTBConfirmation(content)
		fmt.Println("[Node] TB Confirmed by L1/Supervisor.")

	case message.CTakeover:
		r.handleTakeoverCommand(content)
		fmt.Println("[CS_l] Received Takeover Authorization from L1.")
		// 在这里切换本地交易池逻辑，开始接收目标分片的交易

	case message.CReconfig:
		var info message.ReconfigInfo
		json.Unmarshal(content, &info)

		fmt.Printf("[Node %d] RECONFIG START (Epoch %d)...\n", r.pbftNode.NodeID, info.Epoch)
		fmt.Printf("  >> Old Identity: Shard %d, Node %d\n", r.pbftNode.ShardID, r.pbftNode.NodeID)
		fmt.Printf("  >> New Identity: Shard %d, Node %d\n", info.NewShardID, info.NewNodeID)

		// 1. 更新身份信息
		r.pbftNode.ShardID = info.NewShardID
		r.pbftNode.NodeID = info.NewNodeID
		r.pbftNode.RunningNode.ShardID = info.NewShardID
		r.pbftNode.RunningNode.NodeID = info.NewNodeID

		// 2. 更新监督任务
		r.pbftNode.RunningNode.SuperviseTargetShardID = info.MainTarget
		r.pbftNode.RunningNode.BackupTargetShardIDs = info.BackupTargets

		// 3. 更新本地的网络映射表 (IP Table)
		// 这一步至关重要，否则节点无法联系到新分片的队友
		// params.IPmap_nodeTable 是全局变量，可以直接更新
		params.IPmap_nodeTable = info.NewIPTable
		// 更新 pbftNode 内部的引用
		r.pbftNode.ip_nodeTable = info.NewIPTable

		r.mapLock.Lock()
		r.fullBSGMap = info.FullBSGMap
		r.mapLock.Unlock()

		fmt.Printf("  >> Updated Full BSG Map. Total targets tracked: %d\n", len(r.fullBSGMap))

		// 4. 重置共识状态 (新 Epoch 开始)
		// 清空旧的消息池、重置 View、Sequence 等
		r.pbftNode.seqMapLock.Lock()
		r.pbftNode.sequenceID = 0 // 或者从新 Epoch 的基数开始
		r.pbftNode.requestPool = make(map[string]*message.Request)
		r.pbftNode.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
		r.pbftNode.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
		r.pbftNode.seqMapLock.Unlock()

		if r.pbftNode.NodeID == 0 {
			fmt.Println("  >> I am the LEADER of this Epoch.")
			// 如果 Propose 循环依赖于 NodeID，它会自动适配，或者需要在此处重启 Propose
		}

		fmt.Println("RECONFIG COMPLETE.")

	case message.CVoteBSG:
		r.handleBSGVote(content)
	}
}

// 核心逻辑: 判断是否有资格签名 (Top 10% AND VRF Selected)
func (r *ReputationSupervisionMod) checkSigningEligibility(height uint64) bool {
	// 设定选中概率阈值
	// 例如 0.3 表示大约 30% 的节点会被选中
	// 这个值可以根据分片大小动态调整，确保至少有 k 个节点被选中
	const SelectionThreshold = 0.3

	// 直接调用 VRF 进行随机验证
	// 输入: 我的 NodeID, 当前高度 (作为随机种子), 阈值
	// 只要 VRF 输出 < Threshold，即视为中签
	isSelected := utils.VRFVerify(r.pbftNode.NodeID, int64(height), SelectionThreshold)

	if isSelected {
		fmt.Printf("[Node %d]  VRF Selected for Height %d\n", r.pbftNode.NodeID, height)
	}

	return isSelected
}

// 监督逻辑: 检查是否触发计时器
func (r *ReputationSupervisionMod) checkSupervisionTrigger(content []byte) {
	// 1. 解析
	var req message.GetStateMsg
	json.Unmarshal(content, &req)

	// 2. 检查是否归我管
	if req.SenderShardID != r.pbftNode.RunningNode.SuperviseTargetShardID {
		return
	}
	// 3. 只有 Leader 负责计时
	if !r.IsPrimary() {
		return
	}

	fmt.Printf("[Supervisor %d] Monitoring Shard %d start consensus.\n", r.pbftNode.ShardID, req.SenderShardID)
	r.superviseLock.Lock()
	if r.monitorTimer != nil {
		r.monitorTimer.Stop()
	}
	r.monitorTimer = time.AfterFunc(time.Duration(params.SupervisionTimeout)*time.Millisecond, func() {
		r.reportFailure("Block Generation Timeout", req.SenderShardID)
	})
	r.superviseLock.Unlock()
}

func (r *ReputationSupervisionMod) startBlockTimer(targetID uint64) {
	// 实现 map 类型的 timer 管理...
	fmt.Printf("[Supervisor] Timer started for Target %d\n", targetID)
	// time.AfterFunc(..., func() { r.reportFailure(..., targetID) })
}

// 上报失效并请求接管
func (r *ReputationSupervisionMod) reportFailure(reason string, targetShardID uint64) {
	fmt.Printf("[Supervisor %d] Timeout! Reporting failure of Shard %d. Reason: %s\n", r.pbftNode.ShardID, targetShardID, reason)

	currentPoolSize := 0
	if r.pbftNode.CurChain != nil && r.pbftNode.CurChain.Txpool != nil {
		currentPoolSize = r.pbftNode.CurChain.Txpool.GetTxQueueLen()
	}

	report := message.SupervisionResultMsg{
		ReporterShardID: r.pbftNode.ShardID,
		TargetShardID:   targetShardID,
		IsFaulty:        true,
		Reason:          reason, // 使用传入的 reason
		CurrentPoolSize: currentPoolSize,
		TargetPoolSize:  100, // 模拟值
		ReporterNode:    r.pbftNode.RunningNode,
	}
	bytes, _ := json.Marshal(report)
	msg := message.MergeMessage(message.CSupervisionRes, bytes)
	networks.TcpDial(msg, params.SupervisorAddr)
}

func (r *ReputationSupervisionMod) handleBlockHeaderSupervision(content []byte) {
	var bim message.BlockInfoMsg
	json.Unmarshal(content, &bim)

	// 1. 检查是否是我监督的分片
	if bim.SenderShardID != r.pbftNode.RunningNode.SuperviseTargetShardID {
		return
	}
	if !r.IsPrimary() {
		return
	}

	// 2. 收到区块头，说明出块正常，停止出块计时器
	r.superviseLock.Lock()
	if r.monitorTimer != nil {
		r.monitorTimer.Stop()
	}

	fmt.Printf("[Supervisor %d] Block generated by Shard %d. Starting TB Confirmation Timer.\n", r.pbftNode.ShardID, bim.SenderShardID)

	// 3. 启动 TB 确认计时器
	if r.tbMonitorTimer != nil {
		r.tbMonitorTimer.Stop()
	}
	// TB 确认通常涉及 L1 交互，超时时间设长一点
	r.tbMonitorTimer = time.AfterFunc(time.Duration(params.SupervisionTimeout*2)*time.Millisecond, func() {
		r.reportFailure("Timeout: TB Confirmation Failed", bim.SenderShardID)
	})
	r.superviseLock.Unlock()
}

// 处理 TB 确认消息 (停止 TB 计时)
func (r *ReputationSupervisionMod) handleTBConfirmation(content []byte) {
	// 解析消息找到来源分片 (假设 ConfirmTB 消息体包含 ShardID)
	// 这里简单处理，假设收到的广播都是相关的

	r.superviseLock.Lock()
	if r.tbMonitorTimer != nil {
		r.tbMonitorTimer.Stop()
		fmt.Printf("[Supervisor %d] TB Confirmed. Supervision Cycle Completed.\n", r.pbftNode.ShardID)
	}
	r.superviseLock.Unlock()
}

func (r *ReputationSupervisionMod) handleTakeoverCommand(content []byte) {
	fmt.Printf("[CS %d] ACTIVATING TAKEOVER MODE for Shard %d\n", r.pbftNode.ShardID, r.pbftNode.RunningNode.SuperviseTargetShardID)

	r.isTakingOver = true
	r.takeoverTarget = r.pbftNode.RunningNode.SuperviseTargetShardID

}
func (r *ReputationSupervisionMod) initVoting() {
	r.voteBox = make(map[uint64][]*message.BSGVoteMsg)
	r.decisionMade = make(map[uint64]bool)
}

func (r *ReputationSupervisionMod) castBSGVote(targetShardID uint64, isFaulty bool) {
	// 1. 获取 BSG Leader (声誉最高的分片)
	// 在实际实现中，这个信息应该在 EpochReconfiguration 时由 L1 下发
	// 这里假设 r.pbftNode.RunningNode.BSGLeaderMap[targetShardID] 存储了 Leader ID
	// 如果没有该信息，简化方案：广播给 BSG 所有成员，每个人都计算

	// 构造投票消息
	vote := message.BSGVoteMsg{
		VoterShardID:    r.pbftNode.ShardID,
		TargetShardID:   targetShardID,
		IsFaulty:        isFaulty,
		VoterReputation: r.pbftNode.RunningNode.Reputation, // 使用当前声誉
		Timestamp:       time.Now().Unix(),
	}
	voteBytes, _ := json.Marshal(vote)
	msg := message.MergeMessage(message.CVoteBSG, voteBytes)

	// 2. 发送给 BSG Leader (或广播给 BSG 全员)
	// 假设我们有一个函数 getBSGMembers(targetID) 返回成员列表
	// 为了演示，这里假设广播给 BSG 所有成员
	bsgMembers := r.getBSGMembers(targetShardID)
	leaderShardID := r.findHighestReputationShard(bsgMembers)
	if r.pbftNode.ShardID == leaderShardID {
		// 如果我自己就是 Leader，直接内部处理
		r.handleBSGVote(voteBytes)
	} else {
		// 如果我是普通成员，只发送给 Leader (O(1) 的跨分片通信)
		fmt.Printf("[BSG Node %d] Sending vote to BSG Leader %d.\n", r.pbftNode.ShardID, leaderShardID)
		r.broadcastToShard(leaderShardID, msg) // 实际上是发送给那个分片的节点

		// 3. 启动超时防“装死”机制 (借鉴 PBFT View Change)
		r.startLeaderWatchdog(targetShardID, voteBytes)
	}
}
func (r *ReputationSupervisionMod) getReputation(shardID uint64) float64 {
	r.superviseLock.Lock()
	defer r.superviseLock.Unlock()

	// 尝试从本地信誉表中获取该分片的信誉值
	if rep, ok := r.shardRepTable[shardID]; ok {
		return rep
	}

	// 如果没有查到（例如系统刚启动，或者模拟器环境下未完全初始化）
	// 返回一个默认的基础信誉值
	return 1.0
}
func (r *ReputationSupervisionMod) findHighestReputationShard(members []uint64) uint64 {
	var maxRep float64 = -1.0
	var leader uint64 = members[0]
	for _, m := range members {
		rep := r.getReputation(m) // 假设有获取声誉的辅助方法
		if rep > maxRep {
			maxRep = rep
			leader = m
		}
	}
	return leader
}

func (r *ReputationSupervisionMod) startLeaderWatchdog(targetShardID uint64, originalVote []byte) {
	// 设置超时时间 (例如 3 倍的正常网络延迟)
	timeout := time.Duration(params.SupervisionTimeout*3) * time.Millisecond

	time.AfterFunc(timeout, func() {
		// 检查是否已经接到了 L1 的接管通知
		if !r.decisionMade[targetShardID] {
			fmt.Printf("[BSG Node %d] BSG Leader Timeout! Sending vote DIRECTLY to L1.\n", r.pbftNode.ShardID)

			// 把本应发给 Leader 的选票，打包直接发给 L1 (Supervisor)
			// L1 在 handleMessage 中可以额外增加一个直接计票的逻辑
			msg := message.MergeMessage(message.CVoteBSG, originalVote)
			networks.TcpDial(msg, params.SupervisorAddr)
		}
	})
}
func (r *ReputationSupervisionMod) handleBSGVote(content []byte) {
	var vote message.BSGVoteMsg
	json.Unmarshal(content, &vote)

	r.voteLock.Lock()
	defer r.voteLock.Unlock()

	targetID := vote.TargetShardID

	// 防止重复处理已决议的目标
	if r.decisionMade[targetID] {
		return
	}

	// 存入投票箱
	if _, ok := r.voteBox[targetID]; !ok {
		r.voteBox[targetID] = make([]*message.BSGVoteMsg, 0)
	}

	// 检查是否重复投票 (实际应检查 VoterShardID)
	for _, v := range r.voteBox[targetID] {
		if v.VoterShardID == vote.VoterShardID {
			return // 已投过
		}
	}

	r.voteBox[targetID] = append(r.voteBox[targetID], &vote)

	// 尝试计票
	r.tryTallyVotes(targetID)
}

func (r *ReputationSupervisionMod) tryTallyVotes(targetID uint64) {
	votes := r.voteBox[targetID]

	// 1. 计算 BSG 成员的总声誉 (固定的分母)
	var sumTotalBSGRep float64 = 0.0
	bsgMembers := r.getBSGMembers(targetID)

	// 无论成员是否投票、是否装死，分母必须是 BSG 全员的声誉总和
	for _, memberID := range bsgMembers {
		sumTotalBSGRep += r.getReputation(memberID)
	}

	// 2. 统计当前已收到的加权票数 (分子)
	var sumFaultyRep float64 = 0.0
	var sumNormalRep float64 = 0.0

	for _, v := range votes {
		if v.IsFaulty {
			sumFaultyRep += v.VoterReputation
		} else {
			sumNormalRep += v.VoterReputation
		}
	}

	// 设定 PBFT 式的加权法定人数阈值 (例如 2/3)
	const DecisionThreshold = 0.66

	fmt.Printf("[BSG Leader %d] Tallying Target %d: FaultyRep=%.2f, NormalRep=%.2f, TotalBSGRep=%.2f\n",
		r.pbftNode.ShardID, targetID, sumFaultyRep, sumNormalRep, sumTotalBSGRep)

	// -------------------------------------------------------------------------
	// 3. 核心判断：信誉加权达标即通过，无需等待其余选票 (完美解决"装死"问题)
	// -------------------------------------------------------------------------
	if sumFaultyRep >= DecisionThreshold*sumTotalBSGRep {
		fmt.Printf("[BSG Leader] ⚖️ QUORUM REACHED: Shard %d is FAULTY. Action triggered!\n", targetID)

		r.decisionMade[targetID] = true

		// 向 L1 汇报最终结果
		r.reportSupervisorFailure("BSG Quorum Reached: Faulty", targetID)

		// 清理投票箱
		delete(r.voteBox, targetID)

	} else if sumNormalRep >= DecisionThreshold*sumTotalBSGRep {
		fmt.Printf("[BSG Leader] ⚖️ QUORUM REACHED: Shard %d is NORMAL. False alarm cleared.\n", targetID)

		r.decisionMade[targetID] = true
		// 目标正常，无需向 L1 上报接管，直接清理内存即可
		delete(r.voteBox, targetID)
	}
}
func (r *ReputationSupervisionMod) getBSGMembers(targetID uint64) []uint64 {

	return r.fullBSGMap[targetID]
}
func (r *ReputationSupervisionMod) reportSupervisorFailure(reason string, targetID uint64) {
	report := message.SupervisionResultMsg{
		ReporterShardID: r.pbftNode.ShardID,
		TargetShardID:   targetID,
		IsFaulty:        true,
		Reason:          reason,
		ReporterNode:    r.pbftNode.RunningNode,
	}
	bytes, _ := json.Marshal(report)
	msg := message.MergeMessage(message.CSupervisionRes, bytes)
	networks.TcpDial(msg, params.SupervisorAddr)
}
