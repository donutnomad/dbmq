package consumergroup

import (
	"time"
)

// Generation 消费组代际领域实体
type Generation struct {
	GroupID      string    // 消费组ID
	GenerationID uint      // 代际ID
	ProtocolType string    // 协议类型
	LeaderID     string    // Leader消费者ID
	UpdatedAt    time.Time // 更新时间
}
