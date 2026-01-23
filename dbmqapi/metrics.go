package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/samber/lo"
)

// MetricsConfig 监控指标配置
type MetricsConfig struct {
	DB interfaces.DB // 数据库连接
}

// MetricsClient 监控指标客户端，提供兼容Kafka UI的统计接口
// 模仿Kafka的监控指标设计模式
type MetricsClient struct {
	db            interfaces.DB
	topicRepo     topic.Repo
	messageRepo   message.Repo
	heartbeatRepo heartbeat.Repo
	groupRepo     consumergroup.Repo
	progressRepo  consumerprogress.Repo
}

// NewMetricsClient 创建新的监控指标客户端实例
func NewMetricsClient(db interfaces.DB) (*MetricsClient, error) {
	return &MetricsClient{
		db:            db,
		topicRepo:     topicrepo.New(db),
		messageRepo:   messagerepo.New(db),
		heartbeatRepo: heartbeatrepo.New(db),
		groupRepo:     consumergrouprepo.New(db),
		progressRepo:  consumerprogressrepo.New(db),
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
	TopicName      string             `json:"topicName"`      // Topic名称
	PartitionCount int                `json:"partitionCount"` // 分区数量
	MessageCount   int64              `json:"messageCount"`   // 消息总数
	LatestOffset   int64              `json:"latestOffset"`   // 最新偏移量
	SizeBytes      int64              `json:"sizeBytes"`      // 存储大小（字节）
	Partitions     []PartitionMetrics `json:"partitions"`     // 分区详细信息
	Config         map[string]string  `json:"config"`         // Topic配置
	CreatedAt      time.Time          `json:"createdAt"`      // 创建时间
}

// PartitionMetrics 分区信息
type PartitionMetrics struct {
	Partition    int   `json:"partition"`    // 分区ID
	LatestOffset int64 `json:"latestOffset"` // 最新偏移量
	MessageCount int64 `json:"messageCount"` // 消息数量
	SizeBytes    int64 `json:"sizeBytes"`    // 存储大小
}

// ConsumerGroupMetrics 消费组监控指标
type ConsumerGroupMetrics struct {
	GroupID        string                  `json:"groupId"`        // 消费组ID
	State          string                  `json:"state"`          // 状态（Active/Dead）
	Members        []ConsumerMemberMetrics `json:"members"`        // 成员信息
	Lag            int64                   `json:"lag"`            // 总延迟
	PartitionLags  []PartitionLagMetrics   `json:"partitionLags"`  // 分区延迟详情
	LastHeartbeat  time.Time               `json:"lastHeartbeat"`  // 最后心跳时间
	GenerationID   int64                   `json:"generationId"`   // 代际ID
	ProtocolType   string                  `json:"protocolType"`   // 协议类型
	AssignedTopics []string                `json:"assignedTopics"` // 分配的Topic
}

// ConsumerMemberMetrics 消费者成员信息
type ConsumerMemberMetrics struct {
	ConsumerID    string                `json:"consumerId"`    // 消费者ID
	ClientID      string                `json:"clientId"`      // 客户端ID
	Host          string                `json:"host"`          // 主机地址
	LastHeartbeat time.Time             `json:"lastHeartbeat"` // 最后心跳
	Assignment    []types.PartitionInfo `json:"assignment"`    // 分区分配
}

// PartitionLagMetrics 分区延迟信息
type PartitionLagMetrics struct {
	Topic                      string  `json:"topic"`                 // Topic名称
	Partition                  int     `json:"partition"`             // 分区ID
	CurrentOffset              int64   `json:"currentOffset"`         // 当前偏移量
	LatestOffset               int64   `json:"latestOffset"`          // 最新偏移量
	Lag                        int64   `json:"lag"`                   // 延迟数量
	SubscriptionStartWatermark int64   `json:"initialTopicWatermark"` // 消费组首次加入topic时的最新消息ID
	TotalMessageCount          int64   `json:"totalMessageCount"`     // 总消息数
	LastMessageId              int64   `json:"lastMessageId"`
	ConsumedMessages           int64   `json:"consumedMessages"`
	RemainingMessages          int64   `json:"remainingMessages"`
	ConsumedPercentage         float64 `json:"consumedPercentage"`
	UpdatedAt                  int64   `json:"updatedAt"`
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
	var topics []topicrepo.TopicPO
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
	err = mc.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).Count(&messageCount).Error
	if err != nil {
		return nil, fmt.Errorf("failed to get message count: %w", err)
	}
	metrics.MessageCount = messageCount

	// 获取活跃消费组数量
	activeGroups, err := mc.groupRepo.FindAllActiveGroups(ctx, 30*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to get active groups: %w", err)
	}
	metrics.ConsumerGroups = len(activeGroups)

	// 获取活跃消费者数量
	var activeConsumers int64
	cutoff := time.Now().Add(-30 * time.Second)
	err = mc.db.WithContext(ctx).Model(&heartbeatrepo.HeartbeatPO{}).
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
	topicEntity, err := mc.topicRepo.Get(ctx, topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic %s: %w", topicName, err)
	}
	if topicEntity == nil {
		return nil, fmt.Errorf("topic %s not found", topicName)
	}

	metrics := &TopicMetrics{
		TopicName:      topicEntity.Name,
		PartitionCount: int(topicEntity.PartitionCount),
		CreatedAt:      topicEntity.CreatedAt,
		Config:         make(map[string]string),
	}

	// 解析Topic配置
	for k, v := range topicEntity.Configs {
		metrics.Config[k] = fmt.Sprintf("%v", v)
	}

	// 获取分区详细信息
	partitions := make([]PartitionMetrics, topicEntity.PartitionCount)
	var totalMessages int64
	var totalSize int64

	for i := uint(0); i < topicEntity.PartitionCount; i++ {
		// 获取分区最新偏移量
		latestOffset, err := mc.messageRepo.GetLatestID(ctx, topicName, i)
		if err != nil {
			return nil, fmt.Errorf("failed to get latest offset for partition %d: %w", i, err)
		}

		// 获取分区消息数量
		var messageCount int64
		err = mc.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
			Where("topic = ? AND `partition` = ?", topicName, i).
			Count(&messageCount).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get message count for partition %d: %w", i, err)
		}

		// 获取分区存储大小（估算）
		var sizeBytes int64
		err = mc.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
			Select("COALESCE(SUM(LENGTH(body)), 0)").
			Where("topic = ? AND `partition` = ?", topicName, i).
			Scan(&sizeBytes).Error
		if err != nil {
			return nil, fmt.Errorf("failed to get size for partition %d: %w", i, err)
		}

		partitions[i] = PartitionMetrics{
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
		Members:        []ConsumerMemberMetrics{},
		PartitionLags:  []PartitionLagMetrics{},
	}

	// 获取消费组代际信息
	generation, err := mc.groupRepo.GetGeneration(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get consumer group generation: %w", err)
	}
	if generation != nil {
		metrics.GenerationID = int64(generation.GenerationID) // 修复类型转换：uint -> int64
	}

	// 获取活跃消费者
	consumers, err := mc.heartbeatRepo.FindAll(ctx, groupID, 30*time.Second)
	if err != nil {
		return nil, err
	}
	var onlineCount = 0
	for _, item := range consumers {
		// 转换 heartbeatrepo.PartitionInfo 到 types.PartitionInfo
		assignment := make([]types.PartitionInfo, len(item.AssignedPartitions))
		for i, p := range item.AssignedPartitions {
			assignment[i] = types.PartitionInfo{Topic: p.Topic, Partition: p.Partition}
		}
		member := ConsumerMemberMetrics{
			ConsumerID:    item.ConsumerID,
			ClientID:      item.ConsumerID, // DBMQ中ConsumerID就是ClientID
			Host:          "localhost",     // DBMQ单实例
			LastHeartbeat: item.LastHeartbeat,
			Assignment:    assignment,
		}
		metrics.Members = append(metrics.Members, member)
		if item.Offline {
			continue
		}
		onlineCount++
		metrics.LastHeartbeat = item.LastHeartbeat
	}
	if onlineCount == 0 {
		metrics.State = "Dead"
	} else {
		metrics.State = "Active"
	}

	var slices []consumerprogressrepo.ProgressPO
	mc.db.Model(&consumerprogressrepo.ProgressPO{}).Where("group_id = ?", groupID).Find(&slices)

	metrics.AssignedTopics = lo.Uniq(lo.Map(slices, func(item consumerprogressrepo.ProgressPO, index int) string {
		return item.Topic
	}))

	// 计算消费延迟
	var totalLag int64
	for _, topic := range metrics.AssignedTopics {
		topicInfo, err := mc.topicRepo.Get(ctx, topic)
		if err != nil || topicInfo == nil {
			continue
		}

		for i := uint(0); i < topicInfo.PartitionCount; i++ {
			partition := types.PartitionInfo{Topic: topic, Partition: i}

			// 获取已提交ID
			committedIDs, err := mc.progressRepo.GetCommittedOffsets(ctx, groupID, []types.PartitionInfo{partition})
			if err != nil {
				continue
			}
			// 获取该Topic+分区的最新的消息ID
			latestID, err := mc.messageRepo.GetLatestID(ctx, topic, i)
			if err != nil {
				continue
			}

			var currentID int64
			var updateAt int64
			if len(committedIDs) > 0 {
				currentID = committedIDs[0].LastConsumedMessageID
				updateAt = committedIDs[0].UpdatedAt.UnixMilli()
			}

			var offsetRecord consumerprogressrepo.ProgressPO
			err = mc.db.WithContext(ctx).
				Where("group_id = ? AND topic = ? AND `partition` = ?", groupID, topic, i).
				First(&offsetRecord).
				Error
			if err != nil {
				continue
			}
			watermark := offsetRecord.SubscriptionStartWatermark
			var lag int64
			err = mc.db.Model(&messagerepo.MessagePO{}).Where("topic = ?", topic).
				Where("`partition` = ?", i).
				Where("id > ?", currentID).Count(&lag).Error
			if err != nil {
				continue
			}
			var totalMessageCount int64
			err = mc.db.Model(&messagerepo.MessagePO{}).Where("topic = ?", topic).
				Where("`partition` = ?", i).
				Count(&totalMessageCount).Error
			if err != nil {
				continue
			}
			var consumedMessages int64
			err = mc.db.Model(&messagerepo.MessagePO{}).Where("topic = ?", topic).
				Where("`partition` = ?", i).
				Where("id >= ? AND id < ?", watermark, currentID).
				Count(&consumedMessages).Error
			if err != nil {
				continue
			}
			var remainingMessages int64
			err = mc.db.Model(&messagerepo.MessagePO{}).Where("topic = ?", topic).
				Where("`partition` = ?", i).
				Where("(id > ?)", currentID).
				Count(&remainingMessages).Error
			if err != nil {
				continue
			}

			partitionLag := PartitionLagMetrics{
				Topic:                      topic,
				Partition:                  int(i),
				CurrentOffset:              currentID,
				LatestOffset:               latestID,
				Lag:                        lag,
				SubscriptionStartWatermark: watermark,
				TotalMessageCount:          totalMessageCount,
				LastMessageId:              latestID,
				ConsumedMessages:           consumedMessages,
				RemainingMessages:          remainingMessages,
				ConsumedPercentage:         (float64(consumedMessages) / float64(consumedMessages+remainingMessages)) * 100,
				UpdatedAt:                  updateAt,
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
	var topics []topicrepo.TopicPO
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
	err = mc.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).Count(&messageCount).Error
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
	topics, err := mc.topicRepo.GetAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all topics: %w", err)
	}

	var metricsSlice []TopicMetrics
	for _, topic := range topics {
		topicMetrics, err := mc.GetTopicMetrics(ctx, topic.Name)
		if err != nil {
			// 记录错误但继续处理其他Topic
			fmt.Printf("Warning: failed to get metrics for topic %s: %v\n", topic.Name, err)
			continue
		}
		metricsSlice = append(metricsSlice, *topicMetrics)
	}

	return metricsSlice, nil
}

// GetAllConsumerGroupsMetrics 获取所有消费组的监控指标
// 兼容Kafka UI的消费组列表页面
func (mc *MetricsClient) GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error) {
	// 获取所有消费组（包括活跃和非活跃的）
	allGroups, err := mc.groupRepo.FindAllGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get all groups: %w", err)
	}

	var metricsSlice []ConsumerGroupMetrics
	for _, groupID := range allGroups {
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
