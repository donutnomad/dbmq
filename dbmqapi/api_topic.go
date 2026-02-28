package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
)

// TopicAPI Topic 管理 API
// @TAG(Topic)
// @PREFIX(/dbmq/api/v1/topics)
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

func NewTopicAPI(deps *Deps) TopicAPI {
	return &topicAPI{deps: deps}
}

type topicAPI struct {
	deps *Deps
}

func (a *topicAPI) List(ctx context.Context, req GetTopicsReq) ([]TopicResp, error) {
	topics, err := a.deps.TopicQuery.GetAllTopicsMetrics(ctx)
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
				// 使用查询层获取分区统计信息
				stats, err := a.deps.TopicQuery.GetPartitionStats(ctx, topic.TopicName, uint(p))
				if err != nil {
					partitionStats[p] = PartitionStats{
						Partition:      uint(p),
						FirstMessageID: -1,
						LastMessageID:  -1,
					}
				} else {
					partitionStats[p] = PartitionStats{
						Partition:      stats.Partition,
						FirstMessageID: stats.FirstMessageID,
						LastMessageID:  stats.LastMessageID,
						MessageCount:   stats.MessageCount,
						SizeBytes:      stats.SizeBytes,
						CreatedAt:      stats.CreatedAt,
						UpdatedAt:      stats.UpdatedAt,
					}
				}
			}
			result[i].PartitionStats = partitionStats
		}
	}

	return result, nil
}

func (a *topicAPI) Get(ctx context.Context, topicName string) (TopicResp, error) {
	topic, err := a.deps.TopicQuery.GetTopicMetrics(ctx, topicName)
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
