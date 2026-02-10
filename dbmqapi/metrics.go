package dbmqapi

import (
	"context"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
)

// MetricsConfig 监控指标配置
type MetricsConfig struct {
	DB interfaces.DB // 数据库连接
}

// MetricsClient 监控指标客户端，提供兼容Kafka UI的统计接口
// 重构后作为 Query 层的薄包装，不再直接依赖仓储层
type MetricsClient struct {
	clusterQuery  query.ClusterQuery
	topicQuery    query.TopicQuery
	consumerQuery query.ConsumerQuery
	startTime     func() int64 // 返回启动时间戳（秒）
}

// NewMetricsClient 创建新的监控指标客户端实例
func NewMetricsClient(db interfaces.DB) (*MetricsClient, error) {
	return &MetricsClient{
		clusterQuery:  query.NewClusterQuery(db),
		topicQuery:    query.NewTopicQuery(db),
		consumerQuery: query.NewConsumerQuery(db),
	}, nil
}

// NewMetricsClientWithStartTime 创建带启动时间的监控指标客户端
func NewMetricsClientWithStartTime(db interfaces.DB, startTime func() int64) (*MetricsClient, error) {
	return &MetricsClient{
		clusterQuery:  query.NewClusterQuery(db),
		topicQuery:    query.NewTopicQuery(db),
		consumerQuery: query.NewConsumerQuery(db),
		startTime:     startTime,
	}, nil
}

// GetClusterMetrics 获取集群级别监控指标
// 兼容Kafka UI的集群概览页面
func (mc *MetricsClient) GetClusterMetrics(ctx context.Context) (*query.ClusterMetrics, error) {
	return mc.clusterQuery.GetClusterMetrics(ctx)
}

// GetTopicMetrics 获取Topic级别监控指标
// 兼容Kafka UI的Topic详情页面
func (mc *MetricsClient) GetTopicMetrics(ctx context.Context, topicName string) (*query.TopicMetrics, error) {
	return mc.topicQuery.GetTopicMetrics(ctx, topicName)
}

// GetConsumerGroupMetrics 获取消费组监控指标
// 兼容Kafka UI的消费组详情页面
func (mc *MetricsClient) GetConsumerGroupMetrics(ctx context.Context, groupID string) (*query.ConsumerGroupMetrics, error) {
	return mc.consumerQuery.GetConsumerGroupMetrics(ctx, groupID)
}

// GetBrokerMetrics 获取Broker监控指标
// 兼容Kafka UI的Broker页面
func (mc *MetricsClient) GetBrokerMetrics(ctx context.Context) (*query.BrokerMetrics, error) {
	return mc.clusterQuery.GetBrokerMetrics(ctx, mc.startTime)
}

// GetAllTopicsMetrics 获取所有Topic的监控指标
// 兼容Kafka UI的Topic列表页面
func (mc *MetricsClient) GetAllTopicsMetrics(ctx context.Context) ([]query.TopicMetrics, error) {
	return mc.topicQuery.GetAllTopicsMetrics(ctx)
}

// GetAllConsumerGroupsMetrics 获取所有消费组的监控指标
// 兼容Kafka UI的消费组列表页面
func (mc *MetricsClient) GetAllConsumerGroupsMetrics(ctx context.Context) ([]query.ConsumerGroupMetrics, error) {
	return mc.consumerQuery.GetAllConsumerGroupsMetrics(ctx)
}
