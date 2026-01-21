package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/interfaces"
)

// TopicAPI Topic 管理 API
// @TAG(Topic)
// @PREFIX(/api/v1/topics)
type TopicAPI interface {
	// List 获取 Topic 列表
	// @GET(/)
	List(ctx context.Context, req GetTopicsReq) ([]TopicResp, error)
	// Get 获取单个 Topic
	// @GET(/{topicName})
	Get(ctx context.Context, topicName string) (TopicResp, error)
	// Create 创建 Topic
	// @POST(/)
	Create(ctx context.Context, req CreateTopicReq) (MessageResp, error)
	// Delete 删除 Topic
	// @DELETE(/{topicName})
	Delete(ctx context.Context, topicName string) (MessageResp, error)
	// GetMetrics 获取 Topic 指标
	// @GET(/{topicName}/metrics)
	GetMetrics(ctx context.Context, topicName string) (TopicResp, error)
}

type topicAPI struct {
	deps *Deps
}

func NewTopicAPI(deps *Deps) TopicAPI {
	return &topicAPI{deps: deps}
}

func (a *topicAPI) List(ctx context.Context, req GetTopicsReq) ([]TopicResp, error) {
	topics, err := a.deps.MetricsClient.GetAllTopicsMetrics(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]TopicResp, len(topics))
	for i, topic := range topics {
		result[i] = TopicResp{
			Name:           topic.TopicName,
			PartitionCount: uint(topic.PartitionCount),
			MessageCount:   topic.MessageCount,
			SizeBytes:      topic.SizeBytes,
			CreatedAt:      topic.CreatedAt.Format(time.RFC3339),
		}

		if req.IncludePartitionStats && topic.PartitionCount > 0 {
			partitionStats := make([]PartitionStats, topic.PartitionCount)
			for p := range topic.PartitionCount {
				stats, err := getPartitionStatsFromDB(ctx, a.deps.DB, topic.TopicName, uint(p))
				if err != nil {
					partitionStats[p] = PartitionStats{
						Partition:      uint(p),
						FirstMessageID: -1,
						LastMessageID:  -1,
					}
				} else {
					partitionStats[p] = *stats
				}
			}
			result[i].PartitionStats = partitionStats
		}
	}

	return result, nil
}

func (a *topicAPI) Get(ctx context.Context, topicName string) (TopicResp, error) {
	topic, err := a.deps.MetricsClient.GetTopicMetrics(ctx, topicName)
	if err != nil {
		return TopicResp{}, err
	}

	return TopicResp{
		Name:           topic.TopicName,
		PartitionCount: uint(topic.PartitionCount),
		MessageCount:   topic.MessageCount,
		SizeBytes:      topic.SizeBytes,
		CreatedAt:      topic.CreatedAt.Format(time.RFC3339),
	}, nil
}

func (a *topicAPI) Create(ctx context.Context, req CreateTopicReq) (MessageResp, error) {
	topicReq := dbmq.NewTopicRequest{
		Name:          req.Name,
		NumPartitions: int(req.NumPartitions),
	}
	if req.Config != nil && req.Config.RetentionMs != nil {
		topicReq.Config = &dbmq.TopicConfig{
			RetentionMs: req.Config.RetentionMs,
		}
	}

	if err := a.deps.AdminClient.CreateTopic(ctx, topicReq); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{
		Message: fmt.Sprintf("Topic %s created successfully", req.Name),
	}, nil
}

func (a *topicAPI) Delete(ctx context.Context, topicName string) (MessageResp, error) {
	if err := a.deps.AdminClient.DeleteTopics(ctx, []string{topicName}); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{
		Message: fmt.Sprintf("Topic %s deleted successfully", topicName),
	}, nil
}

func (a *topicAPI) GetMetrics(ctx context.Context, topicName string) (TopicResp, error) {
	return a.Get(ctx, topicName)
}

// getPartitionStatsFromDB 从数据库获取分区统计信息
func getPartitionStatsFromDB(ctx context.Context, db interfaces.DB, topicName string, partition uint) (*PartitionStats, error) {
	var stats struct {
		FirstMessageID int64  `gorm:"column:first_message_id"`
		LastMessageID  int64  `gorm:"column:last_message_id"`
		MessageCount   int64  `gorm:"column:message_count"`
		SizeBytes      int64  `gorm:"column:size_bytes"`
		CreatedAt      string `gorm:"column:created_at"`
		UpdatedAt      string `gorm:"column:updated_at"`
	}

	sql := `
		SELECT
			COALESCE(MIN(id), -1) AS first_message_id,
			COALESCE(MAX(id), -1) AS last_message_id,
			COUNT(*) AS message_count,
			COALESCE(SUM(LENGTH(body)), 0) AS size_bytes,
			COALESCE(MIN(created_at), '') AS created_at,
			COALESCE(MAX(created_at), '') AS updated_at
		FROM mq_messages
		WHERE topic = ? AND ` + "`partition`" + ` = ?`

	err := db.WithContext(ctx).Raw(sql, topicName, partition).Scan(&stats).Error
	if err != nil {
		return nil, err
	}

	return &PartitionStats{
		Partition:      partition,
		FirstMessageID: stats.FirstMessageID,
		LastMessageID:  stats.LastMessageID,
		MessageCount:   stats.MessageCount,
		SizeBytes:      stats.SizeBytes,
		CreatedAt:      stats.CreatedAt,
		UpdatedAt:      stats.UpdatedAt,
	}, nil
}
