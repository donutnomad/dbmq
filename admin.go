package dbmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/db/migration"
	"github.com/donutnomad/dbmq/internal/repo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"gorm.io/gorm"
)

// AdminClient 管理客户端，用于Topic和分区的管理操作
type AdminClient struct {
	db repo.DB
}

// NewAdminClient 创建新的管理客户端实例
func NewAdminClient(db repo.DB) *AdminClient {
	return &AdminClient{
		db: db,
	}
}

func (ac *AdminClient) InitDB() error {
	return migration.ApplySchemas(ac.db)
}

func (ac *AdminClient) CreateTopicIfNotExist(ctx context.Context, req NewTopicRequest) error {
	err := ac.CreateTopic(ctx, req)
	if err != nil {
		var topicExistsErr *ErrTopicAlreadyExists
		if errors.As(err, &topicExistsErr) {
			return nil
		}
		return err
	}
	return nil
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
func (ac *AdminClient) ListTopics(ctx context.Context) ([]string, error) {
	var topicNames []string
	err := ac.db.WithContext(ctx).Model(&topicrepo.TopicPO{}).Select("topic_name").Scan(&topicNames).Error
	if err != nil {
		return nil, fmt.Errorf("failed to list topics: %w", err)
	}
	return topicNames, nil
}

// DescribeTopics 获取Topic详细信息
// 模仿Kafka AdminClient.DescribeTopics
func (ac *AdminClient) DescribeTopics(ctx context.Context, topicNames []string) (map[string]*TopicDescription, error) {
	if len(topicNames) == 0 {
		return make(map[string]*TopicDescription), nil
	}

	var topics []topicrepo.TopicPO
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
		var config TopicConfig
		if err := json.Unmarshal(topic.Configs, &config); err == nil {
			desc.Config = &config
		}
		result[topic.TopicName] = desc
	}

	// 检查是否有Topic不存在
	for _, name := range topicNames {
		if _, exists := result[name]; !exists {
			return nil, &ErrUnknownTopicOrPartition{
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
			if err := tx.Where("topic = ?", topicName).Delete(&messagerepo.MessagePO{}).Error; err != nil {
				return fmt.Errorf("failed to delete messages for topic %s: %w", topicName, err)
			}

			// 2. 删除消费组偏移量
			if err := tx.Where("topic = ?", topicName).Delete(&consumerprogressrepo.ProgressPO{}).Error; err != nil {
				return fmt.Errorf("failed to delete offsets for topic %s: %w", topicName, err)
			}

			// 3. 删除Topic本身
			result := tx.Where("topic_name = ?", topicName).Delete(&topicrepo.TopicPO{})
			if result.Error != nil {
				return fmt.Errorf("failed to delete topic %s: %w", topicName, result.Error)
			}
			if result.RowsAffected == 0 {
				return &ErrUnknownTopicOrPartition{Topic: topicName}
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
	err := ac.db.Model(&topicrepo.TopicPO{}).Where("topic_name = ?", req.Name).Count(&count).Error
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}
	if count > 0 {
		return &ErrTopicAlreadyExists{TopicName: req.Name}
	}

	return nil
}

// createTopicInDB 在数据库中创建Topic
func (ac *AdminClient) createTopicInDB(ctx context.Context, req NewTopicRequest) error {
	topic := &topicrepo.TopicPO{
		TopicName:      req.Name,
		PartitionCount: uint(req.NumPartitions),
		Configs:        []byte("{}"),
		CreatedAt:      time.Now(),
	}
	if req.Config != nil {
		configBytes, err := json.Marshal(req.Config)
		if err != nil {
			return fmt.Errorf("failed to marshal topic config: %w", err)
		}
		topic.Configs = configBytes
	}
	if err := ac.db.WithContext(ctx).Create(topic).Error; err != nil {
		return fmt.Errorf("failed to create topic '%s': %w", req.Name, err)
	}
	return nil
}
