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
