package pkg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/donutnomad/dbmq/pkg/errors"
	"github.com/donutnomad/dbmq/pkg/types"
	"time"

	"gorm.io/gorm"
)

// AdminConfig 管理客户端配置
type AdminConfig struct {
	DB *gorm.DB // 数据库连接
}

// AdminClient 管理客户端，用于Topic和分区的管理操作
// 模仿Kafka AdminClient的设计模式
type AdminClient struct {
	db *gorm.DB
}

// NewAdminClient 创建新的管理客户端实例
func NewAdminClient(config AdminConfig) (*AdminClient, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	return &AdminClient{
		db: config.DB,
	}, nil
}

// TopicConfig Topic配置结构，模仿Kafka的TopicConfig
type TopicConfig struct {
	RetentionMs    *int64 `json:"retention.ms,omitempty"`    // 消息保留时间（毫秒）
	RetentionHours *int   `json:"retention.hours,omitempty"` // 消息保留时间（小时）
	CleanupPolicy  string `json:"cleanup.policy,omitempty"`  // 清理策略：delete或compact
}

// NewTopicRequest 创建Topic的请求结构
type NewTopicRequest struct {
	Name          string       // Topic名称
	NumPartitions int          // 分区数量
	Config        *TopicConfig // Topic配置（可选）
	ValidateOnly  bool         // 是否仅验证而不实际创建
}

// TopicResult Topic操作的结果
type TopicResult struct {
	Name  string // Topic名称
	Error error  // 操作错误（如果有）
}

// CreateTopicsResult 批量创建Topic的结果
type CreateTopicsResult struct {
	Results []TopicResult // 每个Topic的创建结果
}

// CreateTopic 创建单个Topic
// 模仿Kafka AdminClient.CreateTopics的单Topic版本
func (ac *AdminClient) CreateTopic(ctx context.Context, req NewTopicRequest) error {
	result := ac.CreateTopics(ctx, []NewTopicRequest{req})
	if len(result.Results) > 0 && result.Results[0].Error != nil {
		return result.Results[0].Error
	}
	return nil
}

// CreateTopics 批量创建Topic
// 模仿Kafka AdminClient.CreateTopics的API设计
func (ac *AdminClient) CreateTopics(ctx context.Context, requests []NewTopicRequest) *CreateTopicsResult {
	result := &CreateTopicsResult{
		Results: make([]TopicResult, len(requests)),
	}

	for i, req := range requests {
		result.Results[i] = TopicResult{Name: req.Name}

		// 验证请求参数
		if err := ac.validateTopicRequest(req); err != nil {
			result.Results[i].Error = err
			continue
		}

		// 如果只是验证，跳过实际创建
		if req.ValidateOnly {
			continue
		}

		// 执行Topic创建
		if err := ac.createTopicInDB(ctx, req); err != nil {
			result.Results[i].Error = err
		}
	}

	return result
}

// ListTopics 列出所有Topic
// 模仿Kafka AdminClient.ListTopics
func (ac *AdminClient) ListTopics(ctx context.Context) ([]string, error) {
	var topics []types.Topic
	err := ac.db.WithContext(ctx).Select("topic_name").Find(&topics).Error
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}

	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = topic.TopicName
	}
	return names, nil
}

// DescribeTopics 获取Topic详细信息
// 模仿Kafka AdminClient.DescribeTopics
func (ac *AdminClient) DescribeTopics(ctx context.Context, topicNames []string) (map[string]*TopicDescription, error) {
	if len(topicNames) == 0 {
		return make(map[string]*TopicDescription), nil
	}

	var topics []types.Topic
	err := ac.db.WithContext(ctx).Where("topic_name IN ?", topicNames).Find(&topics).Error
	if err != nil {
		return nil, fmt.Errorf("failed to describe topics: %w", err)
	}

	result := make(map[string]*TopicDescription)
	for _, topic := range topics {
		desc := &TopicDescription{
			Name:          topic.TopicName,
			NumPartitions: int(topic.PartitionCount),
			CreatedAt:     topic.CreatedAt,
		}

		// 解析配置
		if topic.Configs.Valid {
			var config TopicConfig
			if err := json.Unmarshal([]byte(topic.Configs.String), &config); err == nil {
				desc.Config = &config
			}
		}

		result[topic.TopicName] = desc
	}

	// 检查是否有Topic不存在
	for _, name := range topicNames {
		if _, exists := result[name]; !exists {
			return nil, &errors.ErrUnknownTopicOrPartition{
				Topic: name,
			}
		}
	}

	return result, nil
}

// DeleteTopics 删除Topic
// 模仿Kafka AdminClient.DeleteTopics
func (ac *AdminClient) DeleteTopics(ctx context.Context, topicNames []string) error {
	if len(topicNames) == 0 {
		return nil
	}

	// 在事务中执行删除操作
	return ac.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 删除Topic相关的所有数据
		for _, topicName := range topicNames {
			// 1. 删除消息
			if err := tx.Where("topic = ?", topicName).Delete(&types.Message{}).Error; err != nil {
				return fmt.Errorf("failed to delete messages for topic %s: %w", topicName, err)
			}

			// 2. 删除消费组偏移量
			if err := tx.Where("topic = ?", topicName).Delete(&types.ConsumerGroupOffset{}).Error; err != nil {
				return fmt.Errorf("failed to delete offsets for topic %s: %w", topicName, err)
			}

			// 3. 删除Topic本身
			result := tx.Where("topic_name = ?", topicName).Delete(&types.Topic{})
			if result.Error != nil {
				return fmt.Errorf("failed to delete topic %s: %w", topicName, result.Error)
			}
			if result.RowsAffected == 0 {
				return &errors.ErrUnknownTopicOrPartition{Topic: topicName}
			}
		}
		return nil
	})
}

// TopicDescription Topic描述信息
type TopicDescription struct {
	Name          string       `json:"name"`          // Topic名称
	NumPartitions int          `json:"numPartitions"` // 分区数量
	Config        *TopicConfig `json:"config"`        // Topic配置
	CreatedAt     time.Time    `json:"createdAt"`     // 创建时间
}

// Close 关闭管理客户端
func (ac *AdminClient) Close() {
	// AdminClient本身不需要特殊的清理操作
	// 数据库连接由调用者管理
}

// validateTopicRequest 验证Topic创建请求
func (ac *AdminClient) validateTopicRequest(req NewTopicRequest) error {
	if req.Name == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if req.NumPartitions <= 0 {
		return fmt.Errorf("number of partitions must be positive, got %d", req.NumPartitions)
	}

	// 验证Topic名称格式（模仿Kafka的验证规则）
	if len(req.Name) > 255 {
		return fmt.Errorf("topic name is too long, max length is 255 characters")
	}

	// 检查Topic是否已存在
	var count int64
	err := ac.db.Model(&types.Topic{}).Where("topic_name = ?", req.Name).Count(&count).Error
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}
	if count > 0 {
		return &errors.ErrTopicAlreadyExists{TopicName: req.Name}
	}

	return nil
}

// createTopicInDB 在数据库中创建Topic
func (ac *AdminClient) createTopicInDB(ctx context.Context, req NewTopicRequest) error {
	topic := &types.Topic{
		TopicName:      req.Name,
		PartitionCount: uint(req.NumPartitions),
		CreatedAt:      time.Now(),
	}

	// 处理配置
	if req.Config != nil {
		configBytes, err := json.Marshal(req.Config)
		if err != nil {
			return fmt.Errorf("failed to marshal topic config: %w", err)
		}
		topic.Configs = sql.NullString{
			String: string(configBytes),
			Valid:  true,
		}
	}

	err := ac.db.WithContext(ctx).Create(topic).Error
	if err != nil {
		return fmt.Errorf("failed to create topic '%s': %w", req.Name, err)
	}

	return nil
}
