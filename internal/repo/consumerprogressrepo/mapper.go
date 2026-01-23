package consumerprogressrepo

import (
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(p *ProgressPO) *consumerprogress.Progress {
	if p == nil {
		return nil
	}
	return &consumerprogress.Progress{
		GroupID:                    p.GroupID,
		Topic:                      p.Topic,
		Partition:                  p.Partition,
		LastConsumedMessageID:      p.LastConsumedMessageID,
		SubscriptionRegisteredAt:   p.SubscriptionRegisteredAt,
		SubscriptionStartWatermark: p.SubscriptionStartWatermark,
		GenerationID:               p.GenerationID,
		Metadata:                   p.Metadata,
		UpdatedAt:                  p.UpdatedAt,
	}
}

// ToDomainSlice 从数据库模型切片转换为领域实体切片
func ToDomainSlice(progressList []ProgressPO) []*consumerprogress.Progress {
	result := make([]*consumerprogress.Progress, len(progressList))
	for i := range progressList {
		result[i] = ToDomain(&progressList[i])
	}
	return result
}

// ToPO 从领域实体转换为数据库模型
func ToPO(p *consumerprogress.Progress) *ProgressPO {
	if p == nil {
		return nil
	}
	return &ProgressPO{
		GroupID:                    p.GroupID,
		Topic:                      p.Topic,
		Partition:                  p.Partition,
		LastConsumedMessageID:      p.LastConsumedMessageID,
		SubscriptionRegisteredAt:   p.SubscriptionRegisteredAt,
		SubscriptionStartWatermark: p.SubscriptionStartWatermark,
		GenerationID:               p.GenerationID,
		Metadata:                   p.Metadata,
		UpdatedAt:                  p.UpdatedAt,
	}
}
