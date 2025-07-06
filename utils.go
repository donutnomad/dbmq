package dbmq

import (
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/samber/lo"
	"sort"
)

func mapToPartition(assignment map[string][]uint) (ret []db.PartitionInfo) {
	for topic, parts := range assignment {
		for _, pNum := range parts {
			ret = append(ret, db.PartitionInfo{Topic: topic, Partition: pNum})
		}
	}
	return
}

func partitionToMap(ps []db.PartitionInfo) map[string][]uint {
	return lo.GroupByMap(ps, func(item db.PartitionInfo) (string, uint) {
		return item.Topic, item.Partition
	})
}

func isEmpty[Slice ~[]E, E any](s Slice) bool {
	return len(s) == 0
}

func isNotEmpty[Slice ~[]E, E any](s Slice) bool {
	return len(s) > 0
}

// Ordered 定义了可排序的类型约束
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64 |
		~string
}

// 专门的排序函数，用于常见的排序场景

// SortConsumersByID 按消费者ID升序排序消费者列表
// 确保分配的确定性和一致性
func SortConsumersByID(consumers []db.ConsumerHeartbeat) {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})
}

// SortPartitionsByTopicAndPartition 按主题名称和分区号排序分区列表
// 先按主题排序，再按分区号排序，确保分配的逻辑顺序
func SortPartitionsByTopicAndPartition(partitions []db.PartitionInfo) {
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
}
