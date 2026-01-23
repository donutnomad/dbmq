package consumergrouprepo

import (
	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
)

// ToDomain 从数据库模型转换为领域实体
func ToDomain(gen *GenerationPO) *consumergroup.Generation {
	if gen == nil {
		return nil
	}
	return &consumergroup.Generation{
		GroupID:      gen.GroupID,
		GenerationID: gen.GenerationID,
		ProtocolType: gen.ProtocolType,
		LeaderID:     gen.LeaderID,
		UpdatedAt:    gen.UpdatedAt,
	}
}

// ToPO 从领域实体转换为数据库模型
func ToPO(g *consumergroup.Generation) *GenerationPO {
	if g == nil {
		return nil
	}
	return &GenerationPO{
		GroupID:      g.GroupID,
		GenerationID: g.GenerationID,
		ProtocolType: g.ProtocolType,
		LeaderID:     g.LeaderID,
		UpdatedAt:    g.UpdatedAt,
	}
}
