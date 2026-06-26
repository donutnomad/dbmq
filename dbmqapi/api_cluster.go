package dbmqapi

import (
	"context"
	"fmt"
)

// ClusterAPI 集群管理 API
// @TAG(Cluster)
// @PREFIX(/clusters)
type ClusterAPI interface {
	// List 获取集群列表
	// @GET(/)
	List(ctx context.Context) ([]ClusterResp, error)
	// GetMetrics 获取集群指标
	// @GET(/{clusterId}/metrics)
	GetMetrics(ctx context.Context, clusterId string) (ClusterMetricsResp, error)
	// GetBrokers 获取 Broker 列表
	// @GET(/{clusterId}/brokers)
	GetBrokers(ctx context.Context, clusterId string) ([]BrokerResp, error)
}

type clusterAPI struct {
	deps *Deps
}

func NewClusterAPI(deps *Deps) ClusterAPI {
	return &clusterAPI{deps: deps}
}

func (a *clusterAPI) List(ctx context.Context) ([]ClusterResp, error) {
	return []ClusterResp{
		{
			ClusterID:   "dbmq-cluster",
			Name:        "DBMQ Cluster",
			BrokerCount: 1,
			Status:      "online",
		},
	}, nil
}

func (a *clusterAPI) GetMetrics(ctx context.Context, clusterId string) (ClusterMetricsResp, error) {
	metrics, err := a.deps.ClusterQuery.GetClusterMetrics(ctx)
	if err != nil {
		return ClusterMetricsResp{}, err
	}

	return ClusterMetricsResp{
		TopicCount:         metrics.TopicCount,
		PartitionCount:     metrics.PartitionCount,
		ConsumerGroupCount: metrics.ConsumerGroups,
		TotalMessages:      metrics.MessageCount,
		TotalSizeBytes:     0,
	}, nil
}

func (a *clusterAPI) GetBrokers(ctx context.Context, clusterId string) ([]BrokerResp, error) {
	metrics, err := a.deps.GetBrokerMetrics(ctx)
	if err != nil {
		return nil, err
	}

	return []BrokerResp{
		{
			BrokerID: metrics.BrokerID,
			Host:     metrics.Host,
			Port:     metrics.Port,
			Version:  metrics.Version,
			Uptime:   fmt.Sprintf("%ds", metrics.Uptime),
		},
	}, nil
}
