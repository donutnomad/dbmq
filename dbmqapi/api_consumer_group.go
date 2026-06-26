package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
)

// defaultStaleDays 默认 stale 判定阈值：已消费消息时间与最新消息时间至少相差多少天。
const defaultStaleDays uint = 10

// ConsumerGroupAPI 消费组管理 API
// @TAG(Consumer-Group)
// @PREFIX(/consumer-groups)
type ConsumerGroupAPI interface {
	// List 获取消费组列表
	// @GET(/)
	List(ctx context.Context) ([]ConsumerGroupResp, error)
	// ListStaleProgress 获取消费严重滞后的进度列表
	// @GET(/stale-progress)
	ListStaleProgress(ctx context.Context, req ListStaleProgressReq) ([]StaleProgressResp, error)
	// ListDetachedProgress 获取孤立残留进度列表
	// @GET(/detached-progress)
	ListDetachedProgress(ctx context.Context) ([]DetachedProgressResp, error)
	// Get 获取单个消费组
	// @GET(/{groupId})
	Get(ctx context.Context, groupId string) (ConsumerGroupResp, error)
	// TriggerRebalance 强制触发消费组重新均衡
	// @POST(/{groupId}/rebalance)
	TriggerRebalance(ctx context.Context, groupId string) (MessageResp, error)
	// Delete 删除消费组
	// @DELETE(/{groupId})
	Delete(ctx context.Context, groupId string) (MessageResp, error)
	// DeleteStaleProgress 删除指定消费严重滞后进度
	// @DELETE(/{groupId}/stale-progress)
	DeleteStaleProgress(ctx context.Context, groupId string, req DeleteStaleProgressReq) (MessageResp, error)
	// DeleteDetachedProgress 删除指定孤立残留进度
	// @DELETE(/{groupId}/detached-progress)
	DeleteDetachedProgress(ctx context.Context, groupId string, req DeleteDetachedProgressReq) (MessageResp, error)
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

func (a *consumerGroupAPI) ListStaleProgress(ctx context.Context, req ListStaleProgressReq) ([]StaleProgressResp, error) {
	staleDays := req.StaleDays
	if staleDays == 0 {
		staleDays = defaultStaleDays
	}

	items, err := a.deps.ConsumerQuery.GetStaleProgress(ctx, staleDays)
	if err != nil {
		return nil, err
	}

	result := make([]StaleProgressResp, len(items))
	for i, o := range items {
		result[i] = StaleProgressResp{
			GroupID:               o.GroupID,
			Topic:                 o.Topic,
			Partition:             o.Partition,
			LastConsumedMessageID: o.LastConsumedMessageID,
			ConsumedMsgAt:         o.ConsumedMsgAt.Format(time.RFC3339),
			LatestMsgID:           o.LatestMsgID,
			LatestMsgAt:           o.LatestMsgAt.Format(time.RFC3339),
			StaleDays:             o.StaleDays,
			LagCount:              o.LagCount,
			ProgressUpdatedAt:     o.ProgressUpdatedAt.Format(time.RFC3339),
		}
	}
	return result, nil
}

func (a *consumerGroupAPI) ListDetachedProgress(ctx context.Context) ([]DetachedProgressResp, error) {
	items, err := a.deps.ConsumerQuery.GetDetachedProgress(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]DetachedProgressResp, len(items))
	for i, o := range items {
		result[i] = DetachedProgressResp{
			GroupID:               o.GroupID,
			Topic:                 o.Topic,
			Partition:             o.Partition,
			LastConsumedMessageID: o.LastConsumedMessageID,
			ProgressGenerationID:  o.ProgressGenerationID,
			ProgressUpdatedAt:     o.ProgressUpdatedAt.Format(time.RFC3339),
			DetachedDays:          o.DetachedDays,
		}
	}
	return result, nil
}

func (a *consumerGroupAPI) DeleteStaleProgress(ctx context.Context, groupId string, req DeleteStaleProgressReq) (MessageResp, error) {
	staleDays := req.StaleDays
	if staleDays == 0 {
		staleDays = defaultStaleDays
	}

	// 二次校验：使用相同 staleDays 重新查询，确认 (group, topic, partition) 当前仍 stale，避免误删。
	items, err := a.deps.ConsumerQuery.GetStaleProgress(ctx, staleDays)
	if err != nil {
		return MessageResp{}, err
	}

	matched := false
	for _, o := range items {
		if o.GroupID == groupId && o.Topic == req.Topic && o.Partition == req.Partition {
			matched = true
			break
		}
	}
	if !matched {
		return MessageResp{}, fmt.Errorf("progress (group=%s, topic=%s, partition=%d) is no longer stale under staleDays=%d", groupId, req.Topic, req.Partition, staleDays)
	}

	if err := a.deps.ProgressRepo.DeleteByGroupTopicPartition(ctx, groupId, req.Topic, req.Partition); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{Message: "消费严重滞后进度已删除"}, nil
}

func (a *consumerGroupAPI) DeleteDetachedProgress(ctx context.Context, groupId string, req DeleteDetachedProgressReq) (MessageResp, error) {
	// 二次校验：重新查询 detached 列表，确认 (group, topic, partition) 当前仍是 detached，避免误删。
	items, err := a.deps.ConsumerQuery.GetDetachedProgress(ctx)
	if err != nil {
		return MessageResp{}, err
	}

	matched := false
	for _, o := range items {
		if o.GroupID == groupId && o.Topic == req.Topic && o.Partition == req.Partition {
			matched = true
			break
		}
	}
	if !matched {
		return MessageResp{}, fmt.Errorf("progress (group=%s, topic=%s, partition=%d) is not detached", groupId, req.Topic, req.Partition)
	}

	if err := a.deps.ProgressRepo.DeleteByGroupTopicPartition(ctx, groupId, req.Topic, req.Partition); err != nil {
		return MessageResp{}, err
	}

	return MessageResp{Message: "孤立残留进度已删除"}, nil
}
