package consumergroup

import (
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// Generation 消费组代际领域实体
type Generation struct {
	GroupID      string    // 消费组ID
	GenerationID uint      // 代际ID
	ProtocolType string    // 协议类型
	LeaderID     string    // Leader消费者ID
	UpdatedAt    time.Time // 更新时间
}

// FromDB 从数据库模型转换
func FromDB(gen *db.ConsumerGroupGeneration) *Generation {
	if gen == nil {
		return nil
	}
	return &Generation{
		GroupID:      gen.GroupID,
		GenerationID: gen.GenerationID,
		ProtocolType: gen.ProtocolType,
		LeaderID:     gen.LeaderID,
		UpdatedAt:    gen.UpdatedAt,
	}
}
