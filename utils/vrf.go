package utils

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
)

// 模拟 VRF 计算
// 输入: 私钥(此处用NodeID模拟), 种子(BlockHeight + Round)
// 输出: 随机哈希, 证明(此处简化为哈希本身)
func VRFVerify(nodeID uint64, seed int64, threshold float64) bool {
	// 1. 构造输入: NodeID + Seed
	input := make([]byte, 16)
	binary.BigEndian.PutUint64(input[0:8], nodeID)
	binary.BigEndian.PutUint64(input[8:16], uint64(seed))

	// 2. 计算哈希 (模拟 VRF 输出)
	
	h := hmac.New(sha256.New, input)
	h.Write(input)
	hash := h.Sum(nil)

	// 3. 将哈希转为 [0, 1) 的浮点数
	// 取前 8 字节转为 int64，然后除以 MaxUint64
	hashVal := binary.BigEndian.Uint64(hash[:8])
	maxVal := ^uint64(0)

	ratio := float64(hashVal) / float64(maxVal)

	// 4. 判断是否命中阈值
	return ratio < threshold
}
