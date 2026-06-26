package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/query"
)

// TopicAPI Topic 管理 API
// @TAG(Topic)
// @PREFIX(/topics)
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
	// ListConsumerGroups 获取消费此 Topic 的消费组列表
	// @GET(/{topicName}/consumer-groups)
	ListConsumerGroups(ctx context.Context, topicName string) ([]ConsumerGroupResp, error)
}

func NewTopicAPI(deps *Deps) TopicAPI {
	return &topicAPI{deps: deps}
}

type topicAPI struct {
	deps *Deps
}

func buildTopicResponses(topics []query.TopicMetrics, statsByTopic map[string][]query.PartitionStats, includePartitionStats bool) []TopicResp {
	result := make([]TopicResp, len(topics))
	for i, topic := range topics {
		resp := TopicResp{
			Name:           topic.TopicName,
			PartitionCount: uint(topic.PartitionCount),
			MessageCount:   topic.MessageCount,
			SizeBytes:      topic.SizeBytes,
			CreatedAt:      topic.CreatedAt.Format(time.RFC3339),
		}

		if includePartitionStats && topic.PartitionCount > 0 {
			partitionStats := make([]PartitionStats, topic.PartitionCount)
			for partition := range topic.PartitionCount {
				partitionStats[partition] = PartitionStats{
					Partition:      uint(partition),
					FirstMessageID: -1,
					LastMessageID:  -1,
				}
			}
			for _, stat := range statsByTopic[topic.TopicName] {
				if int(stat.Partition) < topic.PartitionCount {
					partitionStats[stat.Partition] = PartitionStats{
						Partition:      stat.Partition,
						FirstMessageID: stat.FirstMessageID,
						LastMessageID:  stat.LastMessageID,
						MessageCount:   stat.MessageCount,
						SizeBytes:      stat.SizeBytes,
						CreatedAt:      stat.CreatedAt,
						UpdatedAt:      stat.UpdatedAt,
					}
				}
			}
			resp.PartitionStats = partitionStats
		}

		result[i] = resp
	}

	return result
}

func buildPartitionStatsResponses(stats []query.PartitionMetricsDTO) []PartitionStats {
	result := make([]PartitionStats, len(stats))
	for i, stat := range stats {
		result[i] = PartitionStats{
			Partition:      uint(stat.Partition),
			FirstMessageID: stat.FirstMessageID,
			LastMessageID:  stat.LatestOffset,
			MessageCount:   stat.MessageCount,
			SizeBytes:      stat.SizeBytes,
		}
		if stat.LatestOffset < 0 {
			result[i].FirstMessageID = -1
			result[i].LastMessageID = -1
		}
	}
	return result
}

func (a *topicAPI) List(ctx context.Context, req GetTopicsReq) ([]TopicResp, error) {
	topics, err := a.deps.TopicQuery.GetAllTopicsMetrics(ctx)
	if err != nil {
		return nil, err
	}

	statsByTopic := map[string][]query.PartitionStats(nil)
	if req.IncludePartitionStats {
		statsByTopic, err = a.deps.TopicQuery.GetAllTopicPartitionStats(ctx)
		if err != nil {
			return nil, err
		}
	}

	return buildTopicResponses(topics, statsByTopic, req.IncludePartitionStats), nil
}

func (a *topicAPI) Get(ctx context.Context, topicName string) (TopicResp, error) {
	topic, err := a.deps.TopicQuery.GetTopicMetrics(ctx, topicName)
	if err != nil {
		return TopicResp{}, err
	}

	return TopicResp{
		Name:           topic.TopicName,
		PartitionCount: uint(topic.PartitionCount),
		PartitionStats: buildPartitionStatsResponses(topic.Partitions),
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
	groups, err := a.deps.ConsumerQuery.GetAllConsumerGroupsMetrics(ctx)
	if err != nil {
		return MessageResp{}, err
	}
	for _, group := range groups {
		for _, assignedTopic := range group.AssignedTopics {
			if assignedTopic == topicName {
				return MessageResp{}, fmt.Errorf("topic %s is subscribed by consumer group %s", topicName, group.GroupID)
			}
		}
		for _, lag := range group.PartitionLags {
			if lag.Topic == topicName {
				return MessageResp{}, fmt.Errorf("topic %s is subscribed by consumer group %s", topicName, group.GroupID)
			}
		}
	}

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

func (a *topicAPI) ListConsumerGroups(ctx context.Context, topicName string) ([]ConsumerGroupResp, error) {
	groups, err := a.deps.ConsumerQuery.GetConsumerGroupsByTopic(ctx, topicName)
	if err != nil {
		return nil, err
	}

	return buildConsumerGroupResponses(groups, true), nil
}
