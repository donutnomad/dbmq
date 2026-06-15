package dbmq

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/donutnomad/dbmq/logger"
	"github.com/samber/lo"
)

// getAllPartitionsForConsumers 收集活跃消费者订阅的所有唯一主题，
// 并返回这些主题的所有分区列表及一个Topic/Partition哈希。
// 该哈希用于检测Topic扩容或缩容等元数据变化。
func getAllPartitionsForConsumers(ctx context.Context, topicRepo topic.Repo, consumers []*heartbeat.Heartbeat) ([]types.PartitionInfo, string, error) {
	// 收集Topic
	var topicNames []string
	for _, consumer := range consumers {
		for _, topicName := range consumer.SubscribedTopics {
			topicNames = append(topicNames, topicName)
		}
	}
	topicNames = lo.Uniq(topicNames)

	// 从数据库查询主题的元数据信息
	dbTopics, err := topicRepo.FindByNames(ctx, topicNames)
	if err != nil {
		return nil, "", fmt.Errorf("failed to find topics by name: %w", err)
	}

	// 为每个主题生成所有分区的完整列表
	var allPartitions []types.PartitionInfo
	sort.Slice(dbTopics, func(i, j int) bool {
		return dbTopics[i].Name < dbTopics[j].Name
	})
	var partitionHashBuilder strings.Builder
	for _, t := range dbTopics {
		if partitionHashBuilder.Len() > 0 {
			partitionHashBuilder.WriteString("|")
		}
		_, _ = fmt.Fprintf(&partitionHashBuilder, "%s:%d", t.Name, t.PartitionCount)
		// 根据主题的分区数量，生成从0到PartitionCount-1的所有分区
		for i := uint(0); i < t.PartitionCount; i++ {
			allPartitions = append(allPartitions, types.PartitionInfo{Topic: t.Name, Partition: i})
		}
	}
	return allPartitions, partitionHashBuilder.String(), nil
}

// canRebalance 检查是否需要对指定消费组进行重新均衡。
// 除了成员集合外，还会比较每个成员的订阅主题、Topic/Partition元数据哈希，
// 以及 generation_id（检测外部触发的重新均衡，如 TriggerRebalance API）。
func canRebalance(ctx context.Context, snapshot *groupSnapshot, manualAssignmentRepo manualassignment.Repo, groupID string, consumers []*heartbeat.Heartbeat, partitionHash string, currentGenerationID uint) bool {
	log := logger.GetLogger().With("component", "coordinator")

	if snapshot == nil {
		return len(consumers) > 0 || partitionHash != ""
	}

	// 检查 generation_id 是否被外部修改（如 TriggerRebalance API 递增了 generations 表）
	if currentGenerationID != snapshot.generationID {
		log.Info("[LEADER] 检测到 generation_id 被外部修改，强制触发 rebalance",
			"group_id", groupID, "snapshot_generation", snapshot.generationID, "current_generation", currentGenerationID,
		)
		return true
	}

	// 检测活跃消费者心跳行 generation 与组 generation 不一致。
	// 心跳行被删除后会被消费者 Upsert 以 generation_id=0 重建，此时成员集合
	// 和组 generation 均无变化，若不检查此项，协调器与消费者会互相认为
	// "无变化"而永久死锁（消费者空 assignment，需重启才恢复）。
	for _, consumer := range consumers {
		if consumer.GenerationID != currentGenerationID {
			log.Warn("[LEADER] 消费者心跳行 generation 与组 generation 不一致，强制触发 rebalance",
				"group_id", groupID, "consumer_id", consumer.ConsumerID,
				"consumer_generation", consumer.GenerationID, "current_generation", currentGenerationID)
			return true
		}
	}

	if !hasSameMemberTopics(snapshot.memberTopics, consumers) {
		return true
	}

	if snapshot.partitionHash != partitionHash {
		return true
	}

	manualAssignments, err := manualAssignmentRepo.GetMatching(ctx, groupID, getHeartbeatConsumerIDs(consumers))
	if err != nil {
		// 查询失败时保守触发 rebalance
		log.Warn("[LEADER] Failed to check manual assignments, triggering rebalance", "error", err)
		return true
	}

	currentManualHash := hashManualAssignments(manualAssignments)
	if snapshot.manualAssignmentHash != currentManualHash {
		log.Info("[LEADER] 🎯 检测到手动分配规则变化，触发 rebalance",
			"group_id", groupID, "old_hash", snapshot.manualAssignmentHash, "new_hash", currentManualHash,
		)
		return true
	}

	return false
}

func hasSameMemberTopics(memberTopics map[string]string, consumers []*heartbeat.Heartbeat) bool {
	if len(consumers) != len(memberTopics) {
		return false
	}

	return lo.EveryBy(consumers, func(consumer *heartbeat.Heartbeat) bool {
		topicHash, ok := memberTopics[consumer.ConsumerID]
		return ok && topicHash == hashSubscribedTopics(consumer.SubscribedTopics)
	})
}

// calculateAssignments 使用稳定的轮询策略在消费者之间分配分区，支持手动分配覆盖。
// 通过对消费者和分区进行排序，确保分配结果是确定性的，并在消费者增减时最小化分区迁移的开销。
// manualAssignments 参数允许为特定消费者指定固定的分区分配，这些分区不会参与自动轮询分配。
func calculateAssignments(
	consumers []*heartbeat.Heartbeat,
	partitions []types.PartitionInfo,
	manualAssignments map[string][]types.PartitionInfo,
) map[string][]types.PartitionInfo {
	assignments := make(map[string][]types.PartitionInfo, len(consumers))
	if len(consumers) == 0 {
		return assignments
	}

	utils.SortHeartbeatsByID(consumers)
	utils.SortPartitionsByTopicAndPartition(partitions)

	manualAssignedPartitions := make(map[types.PartitionInfo]struct{})
	autoConsumerIDs := make([]string, 0, len(consumers))

	for _, consumer := range consumers {
		consumerID := consumer.ConsumerID
		manualParts := manualAssignments[consumerID]

		if len(manualParts) > 0 {
			assignments[consumerID] = manualParts
			for _, partition := range manualParts {
				manualAssignedPartitions[partition] = struct{}{}
			}
			continue
		}

		assignments[consumerID] = []types.PartitionInfo{}
		autoConsumerIDs = append(autoConsumerIDs, consumerID)
	}

	remainingPartitions := lo.Filter(partitions, func(partition types.PartitionInfo, _ int) bool {
		_, assigned := manualAssignedPartitions[partition]
		return !assigned
	})

	if len(autoConsumerIDs) > 0 {
		for i, partition := range remainingPartitions {
			consumerID := autoConsumerIDs[i%len(autoConsumerIDs)]
			assignments[consumerID] = append(assignments[consumerID], partition)
		}
	}

	return assignments
}
