package dbmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"
	"gorm.io/gorm"
)

// MetricsConfig 监控指标配置
type MetricsConfig struct {
	DB *gorm.DB // 数据库连接
}

// MetricsClient 监控指标客户端，提供兼容Kafka UI的统计接口
// 模仿Kafka的监控指标设计模式
type MetricsClient struct {
	db *gorm.DB
}

// NewMetricsClient 创建新的监控指标客户端实例
func NewMetricsClient(config MetricsConfig) (*MetricsClient, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	return &MetricsClient{
		db: config.DB,
	}, nil
}

// ClusterMetrics 集群级别监控指标
type ClusterMetrics struct {
	ClusterID       string    `json:"clusterId"`       // 集群ID
	BrokerCount     int       `json:"brokerCount"`     // Broker数量（固定为1，因为DBMQ是单实例）
	TopicCount      int       `json:"topicCount"`      // Topic总数
	PartitionCount  int       `json:"partitionCount"`  // 分区总数
	MessageCount    int64     `json:"messageCount"`    // 消息总数
	ConsumerGroups  int       `json:"consumerGroups"`  // 消费组数量
	ActiveConsumers int       `json:"activeConsumers"` // 活跃消费者数量
	Timestamp       time.Time `json:"timestamp"`       // 统计时间戳
}

// TopicMetrics Topic级别监控指标
type TopicMetrics struct {
	TopicName      string            `json:"topicName"`      // Topic名称
	PartitionCount int               `json:"partitionCount"` // 分区数量
	MessageCount   int64             `json:"messageCount"`   // 消息总数
	LatestOffset   int64             `json:"latestOffset"`   // 最新偏移量
	SizeBytes      int64             `json:"sizeBytes"`      // 存储大小（字节）
	Partitions     []PartitionInfo   `json:"partitions"`     // 分区详细信息
	Config         map[string]string `json:"config"`         // Topic配置
	CreatedAt      time.Time         `json:"createdAt"`      // 创建时间
}

// PartitionInfo 分区信息
type PartitionInfo struct {
	Partition    int   `json:"partition"`    // 分区ID
	LatestOffset int64 `json:"latestOffset"` // 最新偏移量
	MessageCount int64 `json:"messageCount"` // 消息数量
	SizeBytes    int64 `json:"sizeBytes"`    // 存储大小
}

// ConsumerGroupMetrics 消费组监控指标
type ConsumerGroupMetrics struct {
	GroupID        string               `json:"groupId"`        // 消费组ID
	State          string               `json:"state"`          // 状态（Active/Dead）
	Members        []ConsumerMemberInfo `json:"members"`        // 成员信息
	Lag            int64                `json:"lag"`            // 总延迟
	PartitionLags  []PartitionLag       `json:"partitionLags"`  // 分区延迟详情
	LastHeartbeat  time.Time            `json:"lastHeartbeat"`  // 最后心跳时间
	GenerationID   int64                `json:"generationId"`   // 代际ID
	ProtocolType   string               `json:"protocolType"`   // 协议类型
	AssignedTopics []string             `json:"assignedTopics"` // 分配的Topic
}

// ConsumerMemberInfo 消费者成员信息
type ConsumerMemberInfo struct {
	ConsumerID    string                `json:"consumerId"`    // 消费者ID
	ClientID      string                `json:"clientId"`      // 客户端ID
	Host          string                `json:"host"`          // 主机地址
	LastHeartbeat time.Time             `json:"lastHeartbeat"` // 最后心跳
	Assignment    []types.PartitionInfo `json:"assignment"`    // 分区分配
}

// PartitionLag 分区延迟信息
type PartitionLag struct {
	Topic         string `json:"topic"`         // Topic名称
	Partition     int    `json:"partition"`     // 分区ID
	CurrentOffset int64  `json:"currentOffset"` // 当前偏移量
	LatestOffset  int64  `json:"latestOffset"`  // 最新偏移量
	Lag           int64  `json:"lag"`           // 延迟数量
}

// BrokerMetrics Broker监控指标（DBMQ为单实例，模拟Kafka Broker）
type BrokerMetrics struct {
	BrokerID       int       `json:"brokerId"`       // Broker ID（固定为0）
	Host           string    `json:"host"`           // 主机地址
	Port           int       `json:"port"`           // 端口号
	IsController   bool      `json:"isController"`   // 是否为控制器（固定为true）
	TopicCount     int       `json:"topicCount"`     // Topic数量
	PartitionCount int       `json:"partitionCount"` // 分区数量
	MessageCount   int64     `json:"messageCount"`   // 消息总数
	Uptime         int64     `json:"uptime"`         // 运行时间（秒）
	Version        string    `json:"version"`        // 版本信息
	LastUpdated    time.Time `json:"lastUpdated"`    // 最后更新时间
}

// GetClusterMetrics 获取集群级别监控指标
// 兼容Kafka UI的集群概览页面
func (mc *MetricsClient) GetClusterMetrics(ctx context.Context) (*ClusterMetrics, error) {
	metrics := &ClusterMetrics{
		ClusterID:   "dbmq-cluster",
		BrokerCount: 1, // DBMQ是单实例
		Timestamp:   time.Now(),
	}

	// 获取Topic总数和分区总数
	var topics []types.Topic
	err := mc.db.WithContext(ctx).Find(&topics).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}

	metrics.TopicCount = len(topics)
	for _, topic := range topics {
		metrics.PartitionCount += int(topic.PartitionCount)
	}

	// 获取消息总数
	var messageCount int64
	err = mc.db.WithContext(ctx).Model(&types.Message{}).Count(&messageCount).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get message count: %w", err)
	}
	metrics.MessageCount = messageCount

	// 获取活跃消费组数量
	activeGroups, err := dal.FindAllActiveGroups(ctx, mc.db, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to get active groups: %w", err)
	}
	metrics.ConsumerGroups = len(activeGroups)

	// 获取活跃消费者数量
	var activeConsumers int64
	cutoff := time.Now().Add(-30 * time.Second)
	err = mc.db.WithContext(ctx).Model(&types.ConsumerHeartbeat{}).
		Where("last_heartbeat > ?", cutoff).
		Count(&activeConsumers).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get active consumers: %w", err)
	}
	metrics.ActiveConsumers = int(activeConsumers)

	return metrics, nil
}

// GetTopicMetrics 获取Topic级别监控指标
// 兼容Kafka UI的Topic详情页面
func (mc *MetricsClient) GetTopicMetrics(ctx context.Context, topicName string) (*TopicMetrics, error) {
	// 获取Topic基本信息
	var topic types.Topic
	err := mc.db.WithContext(ctx).Where("topic_name = ?", topicName).First(&topic).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get topic %s: %w", topicName, err)
	}

	metrics := &TopicMetrics{
		TopicName:      topic.TopicName,
		PartitionCount: int(topic.PartitionCount),
		CreatedAt:      topic.CreatedAt,
		Config:         make(map[string]string),
	}

	// 解析Topic配置
	if topic.Configs.Valid {
		var config map[string]interface{}
		if err := json.Unmarshal([]byte(topic.Configs.String), &config); err == nil {
			for k, v := range config {
				metrics.Config[k] = fmt.Sprintf("%v", v)
			}
		}
	}

	// 获取分区详细信息
	partitions := make([]PartitionInfo, topic.PartitionCount)
	var totalMessages int64
	var totalSize int64

	for i := uint(0); i < topic.PartitionCount; i++ {
		// 获取分区最新偏移量
		latestOffset, err := dal.GetLatestOffset(ctx, mc.db, topicName, i)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest offset for partition %d: %w", i, err)
		}

		// 获取分区消息数量
		var messageCount int64
		err = mc.db.WithContext(ctx).Model(&types.Message{}).
			Where("topic = ? AND `partition` = ?", topicName, i).
			Count(&messageCount).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get message count for partition %d: %w", i, err)
		}

		// 获取分区存储大小（估算）
		var sizeBytes int64
		err = mc.db.WithContext(ctx).Model(&types.Message{}).
			Select("COALESCE(SUM(LENGTH(body)), 0)").
			Where("topic = ? AND `partition` = ?", topicName, i).
			Scan(&sizeBytes).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get size for partition %d: %w", i, err)
		}

		partitions[i] = PartitionInfo{
			Partition:    int(i),
			LatestOffset: latestOffset,
			MessageCount: messageCount,
			SizeBytes:    sizeBytes,
		}

		totalMessages += messageCount
		totalSize += sizeBytes
		if latestOffset > metrics.LatestOffset {
			metrics.LatestOffset = latestOffset
		}
	}

	metrics.Partitions = partitions
	metrics.MessageCount = totalMessages
	metrics.SizeBytes = totalSize

	return metrics, nil
}

// GetConsumerGroupMetrics 获取消费组监控指标
// 兼容Kafka UI的消费组详情页面
func (mc *MetricsClient) GetConsumerGroupMetrics(ctx context.Context, groupID string) (*ConsumerGroupMetrics, error) {
	metrics := &ConsumerGroupMetrics{
		GroupID:        groupID,
		ProtocolType:   "consumer",
		AssignedTopics: []string{},
		Members:        []ConsumerMemberInfo{},
		PartitionLags:  []PartitionLag{},
	}

	// 获取消费组代际信息
	generation, err := dal.GetConsumerGroupGeneration(ctx, mc.db, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group generation: %w", err)
	}
	if generation != nil {
		metrics.GenerationID = int64(generation.GenerationID) // 修复类型转换：uint -> int64
	}

	// 获取活跃消费者
	cutoff := time.Now().Add(-30 * time.Second)
	var heartbeats []types.ConsumerHeartbeat
	err = mc.db.WithContext(ctx).
		Where("group_id = ? AND last_heartbeat > ?", groupID, cutoff).
		Find(&heartbeats).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer heartbeats: %w", err)
	}

	if len(heartbeats) > 0 {
		metrics.State = "Active"
		metrics.LastHeartbeat = heartbeats[0].LastHeartbeat
		for _, hb := range heartbeats {
			if hb.LastHeartbeat.After(metrics.LastHeartbeat) {
				metrics.LastHeartbeat = hb.LastHeartbeat
			}
		}
	} else {
		metrics.State = "Dead"
	}

	// 获取消费者成员信息
	for _, hb := range heartbeats {
		var assignment = hb.AssignedPartitions

		member := ConsumerMemberInfo{
			ConsumerID:    hb.ConsumerID,
			ClientID:      hb.ConsumerID, // DBMQ中ConsumerID就是ClientID
			Host:          "localhost",   // DBMQ单实例
			LastHeartbeat: hb.LastHeartbeat,
			Assignment:    assignment,
		}
		metrics.Members = append(metrics.Members, member)

		// 收集分配的Topic
		for _, partition := range assignment {
			found := false
			for _, topic := range metrics.AssignedTopics {
				if topic == partition.Topic {
					found = true
					break
				}
			}
			if !found {
				metrics.AssignedTopics = append(metrics.AssignedTopics, partition.Topic)
			}
		}
	}

	// 计算消费延迟
	var totalLag int64
	for _, topic := range metrics.AssignedTopics {
		// 获取Topic分区信息
		var topicInfo types.Topic
		err := mc.db.WithContext(ctx).Where("topic_name = ?", topic).First(&topicInfo).Error
		if err != nil {
			continue
		}

		for i := uint(0); i < topicInfo.PartitionCount; i++ {
			partition := types.PartitionInfo{Topic: topic, Partition: i}

			// 获取已提交偏移量
			committedOffsets, err := dal.GetCommittedOffsets(ctx, mc.db, groupID, []types.PartitionInfo{partition})
			if err != nil {
				continue
			}

			// 获取最新偏移量
			latestOffset, err := dal.GetLatestOffset(ctx, mc.db, topic, i)
			if err != nil {
				continue
			}

			currentOffset := int64(0)
			if offset, exists := committedOffsets[partition]; exists {
				currentOffset = offset
			}

			lag := latestOffset - currentOffset
			if lag < 0 {
				lag = 0
			}

			partitionLag := PartitionLag{
				Topic:         topic,
				Partition:     int(i),
				CurrentOffset: currentOffset,
				LatestOffset:  latestOffset,
				Lag:           lag,
			}
			metrics.PartitionLags = append(metrics.PartitionLags, partitionLag)
			totalLag += lag
		}
	}
	metrics.Lag = totalLag

	return metrics, nil
}

// GetBrokerMetrics 获取Broker监控指标
// 兼容Kafka UI的Broker页面
func (mc *MetricsClient) GetBrokerMetrics(ctx context.Context) (*BrokerMetrics, error) {
	metrics := &BrokerMetrics{
		BrokerID:     0,
		Host:         "localhost",
		Port:         9092, // 默认Kafka端口
		IsController: true, // DBMQ单实例总是控制器
		Version:      "dbmq-1.0.0",
		LastUpdated:  time.Now(),
		Uptime:       int64(time.Since(time.Now().Add(-24 * time.Hour)).Seconds()), // 模拟运行时间
	}

	// 获取Topic和分区数量
	var topics []types.Topic
	err := mc.db.WithContext(ctx).Find(&topics).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}

	metrics.TopicCount = len(topics)
	for _, topic := range topics {
		metrics.PartitionCount += int(topic.PartitionCount)
	}

	// 获取消息总数
	var messageCount int64
	err = mc.db.WithContext(ctx).Model(&types.Message{}).Count(&messageCount).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get message count: %w", err)
	}
	metrics.MessageCount = messageCount

	return metrics, nil
}

// GetAllTopicsMetrics 获取所有Topic的监控指标
// 兼容Kafka UI的Topic列表页面
func (mc *MetricsClient) GetAllTopicsMetrics(ctx context.Context) ([]TopicMetrics, error) {
	// 获取所有Topic
	topics, err := dal.GetAllTopics(ctx, mc.db)
	if err != nil {
		return nil, fmt.Errorf("failed to get all topics: %w", err)
	}

	var metricsSlice []TopicMetrics
	for _, topic := range topics {
		topicMetrics, err := mc.GetTopicMetrics(ctx, topic.TopicName)
		if err != nil {
			// 记录错误但继续处理其他Topic
			fmt.Printf("Warning: failed to get metrics for topic %s: %v\n", topic.TopicName, err)
			continue
		}
		metricsSlice = append(metricsSlice, *topicMetrics)
	}

	return metricsSlice, nil
}

// GetAllConsumerGroupsMetrics 获取所有消费组的监控指标
// 兼容Kafka UI的消费组列表页面
func (mc *MetricsClient) GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error) {
	// 获取所有活跃消费组
	activeGroups, err := dal.FindAllActiveGroups(ctx, mc.db, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to get active groups: %w", err)
	}

	var metricsSlice []ConsumerGroupMetrics
	for _, groupID := range activeGroups {
		groupMetrics, err := mc.GetConsumerGroupMetrics(ctx, groupID)
		if err != nil {
			// 记录错误但继续处理其他消费组
			fmt.Printf("Warning: failed to get metrics for consumer group %s: %v\n", groupID, err)
			continue
		}
		metricsSlice = append(metricsSlice, *groupMetrics)
	}

	return metricsSlice, nil
}

// Close 关闭监控指标客户端
func (mc *MetricsClient) Close() {
	// MetricsClient本身不需要特殊的清理操作
	// 数据库连接由调用者管理
}
