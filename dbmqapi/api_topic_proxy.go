package dbmqapi

import (
	"context"
	"fmt"
)

// TopicProxyAPI Topic 代理 API（提供简化的 Topic 和 Broker 接口）
// @TAG(Topic-Proxy)
// @PREFIX(/1)
type TopicProxyAPI interface {
	// ListTopics 获取 Topic 列表
	// @GET(/topics)
	ListTopics(ctx context.Context) ([]string, error)
	// GetTopicInfo 获取 Topic 信息
	// @GET(/topics/{topicName})
	GetTopicInfo(ctx context.Context, topicName string) (TopicResp, error)
	// GetPartitions 获取分区信息
	// @GET(/topics/{topicName}/partitions)
	GetPartitions(ctx context.Context, topicName string) ([]PartitionStats, error)
	// ListBrokers 获取 Broker 列表
	// @GET(/brokers)
	ListBrokers(ctx context.Context) ([]BrokerResp, error)
}

type topicProxyAPI struct {
	deps *Deps
}

func NewTopicProxyAPI(deps *Deps) TopicProxyAPI {
	return &topicProxyAPI{deps: deps}
}

func (a *topicProxyAPI) ListTopics(ctx context.Context) ([]string, error) {
	return a.deps.AdminClient.ListTopics(ctx)
}

func (a *topicProxyAPI) GetTopicInfo(ctx context.Context, topicName string) (TopicResp, error) {
	descriptions, err := a.deps.AdminClient.DescribeTopics(ctx, []string{topicName})
	if err != nil {
		return TopicResp{}, err
	}

	desc, exists := descriptions[topicName]
	if !exists || desc == nil {
		return TopicResp{}, fmt.Errorf("topic not found: %s", topicName)
	}

	return TopicResp{
		Name:           desc.Name,
		PartitionCount: uint(desc.NumPartitions),
	}, nil
}

func (a *topicProxyAPI) GetPartitions(ctx context.Context, topicName string) ([]PartitionStats, error) {
	topicMetrics, err := a.deps.TopicQuery.GetTopicMetrics(ctx, topicName)
	if err != nil {
		return nil, err
	}

	result := make([]PartitionStats, len(topicMetrics.Partitions))
	for i, p := range topicMetrics.Partitions {
		result[i] = PartitionStats{
			Partition:    uint(p.Partition),
			MessageCount: p.MessageCount,
		}
	}
	return result, nil
}

func (a *topicProxyAPI) ListBrokers(ctx context.Context) ([]BrokerResp, error) {
	metrics, err := a.deps.GetBrokerMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return []BrokerResp{
		{
			BrokerID: metrics.BrokerID,
			Host:     metrics.Host,
			Port:     metrics.Port,
		},
	}, nil
}
