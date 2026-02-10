package query

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
)

// ClusterQuery 集群级别查询接口
type ClusterQuery interface {
	// GetClusterStats 获取集群统计信息
	GetClusterStats(ctx context.Context) (*ClusterStats, error)
	// GetBrokerStats 获取 Broker 统计信息
	GetBrokerStats(ctx context.Context) (*BrokerStats, error)
	// GetActiveConsumerCount 获取活跃消费者数量
	GetActiveConsumerCount(ctx context.Context, heartbeatTimeout time.Duration) (int, error)
	// GetActiveGroupCount 获取活跃消费组数量
	GetActiveGroupCount(ctx context.Context, heartbeatTimeout time.Duration) (int, error)
	// GetClusterMetrics 获取集群完整监控指标
	GetClusterMetrics(ctx context.Context) (*ClusterMetrics, error)
	// GetBrokerMetrics 获取 Broker 完整监控指标
	GetBrokerMetrics(ctx context.Context, startTime func() int64) (*BrokerMetrics, error)
}

// clusterQueryMySQL 集群查询 MySQL 实现
type clusterQueryMySQL struct {
	db interfaces.DB
}

// NewClusterQuery 创建集群查询实例
func NewClusterQuery(db interfaces.DB) ClusterQuery {
	return &clusterQueryMySQL{db: db}
}

// getBasicStats 获取基础统计信息 (抽取的共享方法)
func (q *clusterQueryMySQL) getBasicStats(ctx context.Context) (topicCount int, partitionCount int, messageCount int64, err error) {
	var topics []topicrepo.TopicPO
	if err = q.db.WithContext(ctx).Find(&topics).Error; err != nil {
		return
	}

	topicCount = len(topics)
	for _, topic := range topics {
		partitionCount += int(topic.PartitionCount)
	}

	err = q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).Count(&messageCount).Error
	return
}

// GetClusterStats 获取集群统计信息
func (q *clusterQueryMySQL) GetClusterStats(ctx context.Context) (*ClusterStats, error) {
	topicCount, partitionCount, messageCount, err := q.getBasicStats(ctx)
	if err != nil {
		return nil, err
	}

	return &ClusterStats{
		TopicCount:     topicCount,
		PartitionCount: partitionCount,
		MessageCount:   messageCount,
		Timestamp:      time.Now(),
	}, nil
}

// GetBrokerStats 获取 Broker 统计信息
func (q *clusterQueryMySQL) GetBrokerStats(ctx context.Context) (*BrokerStats, error) {
	topicCount, partitionCount, messageCount, err := q.getBasicStats(ctx)
	if err != nil {
		return nil, err
	}

	return &BrokerStats{
		TopicCount:     topicCount,
		PartitionCount: partitionCount,
		MessageCount:   messageCount,
	}, nil
}

// GetActiveConsumerCount 获取活跃消费者数量
func (q *clusterQueryMySQL) GetActiveConsumerCount(ctx context.Context, heartbeatTimeout time.Duration) (int, error) {
	var count int64
	cutoff := time.Now().Add(-heartbeatTimeout)
	err := q.db.WithContext(ctx).Model(&heartbeatrepo.HeartbeatPO{}).
		Where("last_heartbeat > ?", cutoff).
		Count(&count).Error
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// GetActiveGroupCount 获取活跃消费组数量
func (q *clusterQueryMySQL) GetActiveGroupCount(ctx context.Context, heartbeatTimeout time.Duration) (int, error) {
	var count int64
	cutoff := time.Now().Add(-heartbeatTimeout)
	err := q.db.WithContext(ctx).Model(&heartbeatrepo.HeartbeatPO{}).
		Where("last_heartbeat > ? AND offline = ?", cutoff, false).
		Distinct("group_id").
		Count(&count).Error
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// GetClusterMetrics 获取集群完整监控指标
func (q *clusterQueryMySQL) GetClusterMetrics(ctx context.Context) (*ClusterMetrics, error) {
	// 获取集群统计信息
	clusterStats, err := q.GetClusterStats(ctx)
	if err != nil {
		return nil, err
	}

	// 获取活跃消费组数量
	activeGroups, err := q.GetActiveGroupCount(ctx, 30*time.Second)
	if err != nil {
		return nil, err
	}

	// 获取活跃消费者数量
	activeConsumers, err := q.GetActiveConsumerCount(ctx, 30*time.Second)
	if err != nil {
		return nil, err
	}

	return &ClusterMetrics{
		ClusterID:       "dbmq-cluster",
		BrokerCount:     1, // DBMQ 是单实例
		TopicCount:      clusterStats.TopicCount,
		PartitionCount:  clusterStats.PartitionCount,
		MessageCount:    clusterStats.MessageCount,
		ConsumerGroups:  activeGroups,
		ActiveConsumers: activeConsumers,
		Timestamp:       clusterStats.Timestamp,
	}, nil
}

// GetBrokerMetrics 获取 Broker 完整监控指标
func (q *clusterQueryMySQL) GetBrokerMetrics(ctx context.Context, startTime func() int64) (*BrokerMetrics, error) {
	brokerStats, err := q.GetBrokerStats(ctx)
	if err != nil {
		return nil, err
	}

	var uptime int64
	if startTime != nil {
		uptime = time.Now().Unix() - startTime()
	} else {
		// 默认 24 小时
		uptime = int64(24 * time.Hour.Seconds())
	}

	return &BrokerMetrics{
		BrokerID:       0,
		Host:           "localhost",
		Port:           9092,
		IsController:   true,
		TopicCount:     brokerStats.TopicCount,
		PartitionCount: brokerStats.PartitionCount,
		MessageCount:   brokerStats.MessageCount,
		Version:        "dbmq-1.0.0",
		LastUpdated:    time.Now(),
		Uptime:         uptime,
	}, nil
}
