package dbmq

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	"github.com/donutnomad/dbmq/internal/types"
	"github.com/samber/lo"
)

// groupLocks 提供基于消费组ID的锁机制
// 确保同一个消费组在同一时间只能进行一次重新均衡操作
type groupLocks struct {
	mu    sync.Mutex
	locks map[string]struct{} // 存储已锁定的消费组ID
}

func newGroupLocks() *groupLocks {
	return &groupLocks{
		locks: make(map[string]struct{}),
	}
}

// TryLock 尝试为给定的消费组ID获取锁
// 返回true表示获取成功，false表示锁已被持有
func (gl *groupLocks) TryLock(groupID string) bool {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	if _, ok := gl.locks[groupID]; ok {
		return false // 锁已被持有
	}
	gl.locks[groupID] = struct{}{}
	return true
}

// Unlock 释放指定消费组ID的锁
func (gl *groupLocks) Unlock(groupID string) {
	gl.mu.Lock()
	defer gl.mu.Unlock()
	delete(gl.locks, groupID)
}

func getHeartbeatConsumerIDs(hs []*heartbeat.Heartbeat) []string {
	return lo.Map(hs, func(item *heartbeat.Heartbeat, index int) string {
		return item.ConsumerID
	})
}

func hashSubscribedTopics(topics []string) string {
	if len(topics) == 0 {
		return ""
	}
	sorted := slices.Clone(topics)
	sort.Strings(sorted)
	return strings.Join(sorted, "|")
}

// hashManualAssignments 计算手动分配规则的哈希值，用于检测规则变化
func hashManualAssignments(assignments map[string][]types.PartitionInfo) string {
	if len(assignments) == 0 {
		return ""
	}

	// 收集所有条目并排序，确保结果可重现
	type entry struct {
		consumerID string
		partitions []types.PartitionInfo
	}

	entries := make([]entry, 0, len(assignments))
	for consumerID, partitions := range assignments {
		// 克隆并排序分区列表
		sortedParts := slices.Clone(partitions)
		utils.SortPartitionsByTopicAndPartition(sortedParts)
		entries = append(entries, entry{
			consumerID: consumerID,
			partitions: sortedParts,
		})
	}

	// 按 consumerID 排序
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].consumerID < entries[j].consumerID
	})

	// 构建哈希字符串
	var parts []string
	for _, e := range entries {
		for _, p := range e.partitions {
			parts = append(parts, fmt.Sprintf("%s:%s:%d", e.consumerID, p.Topic, p.Partition))
		}
	}

	return strings.Join(parts, "|")
}
