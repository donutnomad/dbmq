package dbmqapi

import (
	"context"
)

// ConsumerGroupAPI 消费组管理 API
// @TAG(Consumer-Group)
type ConsumerGroupAPI interface {
	// List 获取消费组列表
	// @GET(/api/v1/consumer-groups)
	List(ctx context.Context) ([]ConsumerGroupResp, error)
	// Get 获取单个消费组
	// @GET(/api/v1/consumer-groups/{groupId})
	Get(ctx context.Context, groupId string) (ConsumerGroupResp, error)
	// TriggerRebalance 强制触发消费组重新均衡
	// @POST(/api/v1/consumer-groups/{groupId}/rebalance)
	TriggerRebalance(ctx context.Context, groupId string) (MessageResp, error)
}

type consumerGroupAPI struct {
	deps *Deps
}

func NewConsumerGroupAPI(deps *Deps) ConsumerGroupAPI {
	return &consumerGroupAPI{deps: deps}
}

func (a *consumerGroupAPI) List(ctx context.Context) ([]ConsumerGroupResp, error) {
	groups, err := a.deps.MetricsClient.GetAllConsumerGroupsMetrics(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]ConsumerGroupResp, len(groups))
	for i, g := range groups {
		partitionLags := make([]PartitionLagResp, len(g.PartitionLags))
		for j, lag := range g.PartitionLags {
			// 计算消费进度
			consumed := lag.CurrentOffset
			remaining := lag.Lag
			var percentage float64
			if lag.LatestOffset > 0 {
				percentage = float64(consumed) / float64(lag.LatestOffset) * 100
			} else {
				percentage = 100
			}

			partitionLags[j] = PartitionLagResp{
				Topic:              lag.Topic,
				Partition:          lag.Partition,
				CurrentOffset:      lag.CurrentOffset,
				LatestOffset:       lag.LatestOffset,
				Lag:                lag.Lag,
				ConsumedMessages:   consumed,
				RemainingMessages:  remaining,
				ConsumedPercentage: percentage,
			}
		}

		result[i] = ConsumerGroupResp{
			GroupID:       g.GroupID,
			State:         g.State,
			MemberCount:   len(g.Members),
			TotalLag:      g.Lag,
			PartitionLags: partitionLags,
		}
	}

	return result, nil
}

func (a *consumerGroupAPI) Get(ctx context.Context, groupId string) (ConsumerGroupResp, error) {
	group, err := a.deps.MetricsClient.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		return ConsumerGroupResp{}, err
	}

	partitionLags := make([]PartitionLagResp, len(group.PartitionLags))
	for i, lag := range group.PartitionLags {
		// 计算消费进度
		consumed := lag.CurrentOffset
		remaining := lag.Lag
		var percentage float64
		if lag.LatestOffset > 0 {
			percentage = float64(consumed) / float64(lag.LatestOffset) * 100
		} else {
			percentage = 100 // 没有消息时视为100%
		}

		partitionLags[i] = PartitionLagResp{
			Topic:              lag.Topic,
			Partition:          lag.Partition,
			CurrentOffset:      lag.CurrentOffset,
			LatestOffset:       lag.LatestOffset,
			Lag:                lag.Lag,
			ConsumedMessages:   consumed,
			RemainingMessages:  remaining,
			ConsumedPercentage: percentage,
		}
	}

	return ConsumerGroupResp{
		GroupID:       group.GroupID,
		State:         group.State,
		MemberCount:   len(group.Members),
		TotalLag:      group.Lag,
		PartitionLags: partitionLags,
	}, nil
}

func (a *consumerGroupAPI) TriggerRebalance(ctx context.Context, groupId string) (MessageResp, error) {
	// 通过递增 generation_id 强制触发 rebalance
	// 消费者检测到 generation 变化后会自动触发重新均衡流程
	err := a.deps.DB.WithContext(ctx).
		Exec("UPDATE mq_consumer_heartbeats SET generation_id = generation_id + 1 WHERE group_id = ?", groupId).
		Error
	if err != nil {
		return MessageResp{}, err
	}

	return MessageResp{
		Message: "重新均衡已触发，更改将在 10 秒内生效",
	}, nil
}
