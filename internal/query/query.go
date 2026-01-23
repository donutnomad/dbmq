package query

import "github.com/donutnomad/dbmq/internal/interfaces"

// Queries 聚合所有查询接口
type Queries struct {
	Cluster  ClusterQuery
	Topic    TopicQuery
	Consumer ConsumerQuery
	Message  MessageQuery
}

// New 创建查询服务集合 (MySQL 实现)
func New(db interfaces.DB) *Queries {
	return &Queries{
		Cluster:  NewClusterQuery(db),
		Topic:    NewTopicQuery(db),
		Consumer: NewConsumerQuery(db),
		Message:  NewMessageQuery(db),
	}
}
