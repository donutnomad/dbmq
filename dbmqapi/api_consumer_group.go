package dbmqapi

import (
	"context"
)

// ConsumerGroupAPI 消费组管理 API
// @TAG(Consumer-Group)
// @PREFIX(/api/v1/consumer-groups)
type ConsumerGroupAPI interface {
	// List 获取消费组列表
	// @GET(/)
	List(ctx context.Context) ([]ConsumerGroupResp, error)
	// Get 获取单个消费组
	// @GET(/{groupId})
	Get(ctx context.Context, groupId string) (ConsumerGroupResp, error)
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
			partitionLags[j] = PartitionLagResp{
				Topic:         lag.Topic,
				Partition:     lag.Partition,
				CurrentOffset: lag.CurrentOffset,
				LatestOffset:  lag.LatestOffset,
				Lag:           lag.Lag,
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
		partitionLags[i] = PartitionLagResp{
			Topic:         lag.Topic,
			Partition:     lag.Partition,
			CurrentOffset: lag.CurrentOffset,
			LatestOffset:  lag.LatestOffset,
			Lag:           lag.Lag,
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
