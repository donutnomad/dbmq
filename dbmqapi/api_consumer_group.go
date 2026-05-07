package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
)

// ConsumerGroupAPI 消费组管理 API
// @TAG(Consumer-Group)
// @PREFIX(/dbmq/api/v1/consumer-groups)
type ConsumerGroupAPI interface {
	// List 获取消费组列表
	// @GET(/)
	List(ctx context.Context) ([]ConsumerGroupResp, error)
	// Get 获取单个消费组
	// @GET(/{groupId})
	Get(ctx context.Context, groupId string) (ConsumerGroupResp, error)
	// TriggerRebalance 强制触发消费组重新均衡
	// @POST(/{groupId}/rebalance)
	TriggerRebalance(ctx context.Context, groupId string) (MessageResp, error)
	// Delete 删除消费组
	// @DELETE(/{groupId})
	Delete(ctx context.Context, groupId string) (MessageResp, error)
}

type consumerGroupAPI struct {
	deps *Deps
}

func NewConsumerGroupAPI(deps *Deps) ConsumerGroupAPI {
	return &consumerGroupAPI{deps: deps}
}

func buildPartitionLagResponses(lags []query.PartitionLagMetrics) []PartitionLagResp {
	partitionLags := make([]PartitionLagResp, len(lags))
	for i, lag := range lags {
		partitionLags[i] = PartitionLagResp{
			Topic:              lag.Topic,
			Partition:          lag.Partition,
			CurrentOffset:      lag.CurrentOffset,
			LatestOffset:       lag.LatestOffset,
			Lag:                lag.Lag,
			ConsumedMessages:   lag.ConsumedMessages,
			RemainingMessages:  lag.RemainingMessages,
			ConsumedPercentage: lag.ConsumedPercentage,
		}
	}
	return partitionLags
}

func buildConsumerMemberResponses(members []query.ConsumerMemberMetrics, includeMembers bool) []ConsumerMemberDTO {
	if !includeMembers || len(members) == 0 {
		return nil
	}

	result := make([]ConsumerMemberDTO, len(members))
	for i, member := range members {
		assignment := make(map[string][]int)
		for _, part := range member.Assignment {
			assignment[part.Topic] = append(assignment[part.Topic], int(part.Partition))
		}

		result[i] = ConsumerMemberDTO{
			MemberID:      member.ConsumerID,
			ClientID:      member.ClientID,
			Host:          member.Host,
			LastHeartbeat: member.LastHeartbeat.Format(time.RFC3339),
			Assignment:    assignment,
		}
	}
	return result
}

func buildConsumerGroupResponses(groups []query.ConsumerGroupMetrics, includeMembers bool) []ConsumerGroupResp {
	result := make([]ConsumerGroupResp, len(groups))
	for i, g := range groups {
		result[i] = ConsumerGroupResp{
			GroupID:       g.GroupID,
			State:         g.State,
			MemberCount:   len(g.Members),
			TotalLag:      g.Lag,
			PartitionLags: buildPartitionLagResponses(g.PartitionLags),
			Members:       buildConsumerMemberResponses(g.Members, includeMembers),
		}
	}
	return result
}

func (a *consumerGroupAPI) List(ctx context.Context) ([]ConsumerGroupResp, error) {
	groups, err := a.deps.ConsumerQuery.GetAllConsumerGroupsSummary(ctx)
	if err != nil {
		return nil, err
	}

	return buildConsumerGroupResponses(groups, false), nil
}

func (a *consumerGroupAPI) Get(ctx context.Context, groupId string) (ConsumerGroupResp, error) {
	group, err := a.deps.ConsumerQuery.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		return ConsumerGroupResp{}, err
	}

	return ConsumerGroupResp{
		GroupID:       group.GroupID,
		State:         group.State,
		MemberCount:   len(group.Members),
		TotalLag:      group.Lag,
		PartitionLags: buildPartitionLagResponses(group.PartitionLags),
	}, nil
}

func (a *consumerGroupAPI) TriggerRebalance(ctx context.Context, groupId string) (MessageResp, error) {
	// 通过递增 mq_consumer_group_generations 表的 generation_id 触发重新均衡。
	// 协调器在下一轮 scan 时会检测到 generation_id 与内存快照不一致，
	// 从而执行完整的 rebalance 流程（重新计算分区分配并更新数据库）。
	_, err := a.deps.ConsumerGroupRepo.IncrementGenerationID(ctx, groupId)
	if err != nil {
		return MessageResp{}, err
	}

	return MessageResp{
		Message: "重新均衡已触发，更改将在下一个检查周期内生效",
	}, nil
}

func (a *consumerGroupAPI) Delete(ctx context.Context, groupId string) (MessageResp, error) {
	activeMembers, err := a.deps.HeartbeatRepo.FindActive(ctx, groupId, defaultHeartbeatTimeout)
	if err != nil {
		return MessageResp{}, err
	}
	if len(activeMembers) > 0 {
		return MessageResp{}, fmt.Errorf("active consumer group cannot be deleted")
	}

	if err := a.deps.ConsumerGroupRepo.Delete(ctx, groupId); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{Message: "消费组已删除"}, nil
}
