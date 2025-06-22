package pkg

import (
	"dbmq/pkg/types"
	"sort"
)

// SortBy 根据提供的比较函数对切片进行排序
// 这是一个泛型函数，可以处理任何类型的切片
func SortBy[T any](slice []T, less func(i, j int) bool) {
	sort.Slice(slice, less)
}

// SortByKey 根据键提取函数对切片进行排序
// 键必须是可比较的类型（实现了 Ordered 接口）
func SortByKey[T any, K Ordered](slice []T, keyFunc func(T) K) {
	sort.Slice(slice, func(i, j int) bool {
		return keyFunc(slice[i]) < keyFunc(slice[j])
	})
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
func SortConsumersByID(consumers []types.ConsumerHeartbeat) {
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerID < consumers[j].ConsumerID
	})
}

// SortPartitionsByTopicAndPartition 按主题名称和分区号排序分区列表
// 先按主题排序，再按分区号排序，确保分配的逻辑顺序
func SortPartitionsByTopicAndPartition(partitions []types.PartitionInfo) {
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
}

// SortStringSlice 对字符串切片进行排序
func SortStringSlice(slice []string) {
	sort.Strings(slice)
}

// SortTopicsByName 按主题名称排序主题列表
func SortTopicsByName(topics []types.Topic) {
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].TopicName < topics[j].TopicName
	})
}

// 高级排序工具

// StableSort 使用稳定排序算法对切片进行排序
// 相等元素的相对顺序保持不变
func StableSort[T any](slice []T, less func(i, j int) bool) {
	sort.SliceStable(slice, less)
}

// IsSorted 检查切片是否已按给定的比较函数排序
func IsSorted[T any](slice []T, less func(i, j int) bool) bool {
	return sort.SliceIsSorted(slice, less)
}

// BinarySearch 在已排序的切片中进行二分查找
// 返回目标元素的索引，如果不存在则返回应该插入的位置
func BinarySearch[T any](slice []T, target T, less func(T, T) bool) int {
	return sort.Search(len(slice), func(i int) bool {
		return !less(slice[i], target)
	})
}

// 多字段排序支持

// MultiFieldSorter 支持多字段排序的结构体
type MultiFieldSorter[T any] struct {
	data []T
	less []func(T, T) bool
}

// NewMultiFieldSorter 创建一个新的多字段排序器
func NewMultiFieldSorter[T any](data []T) *MultiFieldSorter[T] {
	return &MultiFieldSorter[T]{
		data: data,
		less: make([]func(T, T) bool, 0),
	}
}

// OrderBy 添加一个排序字段
func (m *MultiFieldSorter[T]) OrderBy(less func(T, T) bool) *MultiFieldSorter[T] {
	m.less = append(m.less, less)
	return m
}

// Sort 执行多字段排序
func (m *MultiFieldSorter[T]) Sort() {
	sort.Slice(m.data, func(i, j int) bool {
		for _, lessFn := range m.less {
			if lessFn(m.data[i], m.data[j]) {
				return true
			}
			if lessFn(m.data[j], m.data[i]) {
				return false
			}
		}
		return false
	})
}
