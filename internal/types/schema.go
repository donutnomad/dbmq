package types

import (
	"fmt"
)

// PartitionInfo 唯一标识一个Topic-分区对
// 在内存中用作Map的键，用于管理偏移量和分区分配
type PartitionInfo struct {
	Topic     string `json:"Topic"`     // Topic名称
	Partition uint   `json:"Partition"` // 分区号
}

func (p PartitionInfo) String() string {
	return fmt.Sprintf("(topic=%s,partition=%d)", p.Topic, p.Partition)
}
