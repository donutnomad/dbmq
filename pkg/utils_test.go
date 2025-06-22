package pkg

import (
	"dbmq/pkg/types"
	"reflect"
	"testing"
)

func TestSortConsumersByID(t *testing.T) {
	consumers := []types.ConsumerHeartbeat{
		{ConsumerID: "consumer-c"},
		{ConsumerID: "consumer-a"},
		{ConsumerID: "consumer-b"},
	}

	SortConsumersByID(consumers)

	expected := []types.ConsumerHeartbeat{
		{ConsumerID: "consumer-a"},
		{ConsumerID: "consumer-b"},
		{ConsumerID: "consumer-c"},
	}

	if !reflect.DeepEqual(consumers, expected) {
		t.Errorf("SortConsumersByID failed. Got: %v, Want: %v", consumers, expected)
	}
}

func TestSortPartitionsByTopicAndPartition(t *testing.T) {
	partitions := []types.PartitionInfo{
		{Topic: "topic-b", Partition: 1},
		{Topic: "topic-a", Partition: 2},
		{Topic: "topic-b", Partition: 0},
		{Topic: "topic-a", Partition: 1},
	}

	SortPartitionsByTopicAndPartition(partitions)

	expected := []types.PartitionInfo{
		{Topic: "topic-a", Partition: 1},
		{Topic: "topic-a", Partition: 2},
		{Topic: "topic-b", Partition: 0},
		{Topic: "topic-b", Partition: 1},
	}

	if !reflect.DeepEqual(partitions, expected) {
		t.Errorf("SortPartitionsByTopicAndPartition failed. Got: %v, Want: %v", partitions, expected)
	}
}

func TestSortByKey(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	people := []Person{
		{"Alice", 30},
		{"Bob", 25},
		{"Charlie", 35},
	}

	// 按年龄排序
	SortByKey(people, func(p Person) int { return p.Age })

	expected := []Person{
		{"Bob", 25},
		{"Alice", 30},
		{"Charlie", 35},
	}

	if !reflect.DeepEqual(people, expected) {
		t.Errorf("SortByKey failed. Got: %v, Want: %v", people, expected)
	}
}

func TestSortBy(t *testing.T) {
	numbers := []int{3, 1, 4, 1, 5, 9, 2, 6}

	// 降序排序
	SortBy(numbers, func(i, j int) bool {
		return numbers[i] > numbers[j]
	})

	expected := []int{9, 6, 5, 4, 3, 2, 1, 1}

	if !reflect.DeepEqual(numbers, expected) {
		t.Errorf("SortBy failed. Got: %v, Want: %v", numbers, expected)
	}
}

func TestMultiFieldSorter(t *testing.T) {
	type Student struct {
		Grade int
		Name  string
		Age   int
	}

	students := []Student{
		{Grade: 90, Name: "Alice", Age: 20},
		{Grade: 85, Name: "Bob", Age: 22},
		{Grade: 90, Name: "Charlie", Age: 19},
		{Grade: 85, Name: "David", Age: 21},
	}

	// 按成绩降序，然后按年龄升序排序
	sorter := NewMultiFieldSorter(students).
		OrderBy(func(a, b Student) bool { return a.Grade > b.Grade }). // 成绩降序
		OrderBy(func(a, b Student) bool { return a.Age < b.Age })      // 年龄升序

	sorter.Sort()

	expected := []Student{
		{Grade: 90, Name: "Charlie", Age: 19}, // 成绩90，年龄最小
		{Grade: 90, Name: "Alice", Age: 20},   // 成绩90，年龄次小
		{Grade: 85, Name: "David", Age: 21},   // 成绩85，年龄小
		{Grade: 85, Name: "Bob", Age: 22},     // 成绩85，年龄大
	}

	if !reflect.DeepEqual(students, expected) {
		t.Errorf("MultiFieldSorter failed. Got: %v, Want: %v", students, expected)
	}
}

func TestIsSorted(t *testing.T) {
	sortedNumbers := []int{1, 2, 3, 4, 5}
	unsortedNumbers := []int{3, 1, 4, 2, 5}

	if !IsSorted(sortedNumbers, func(i, j int) bool { return sortedNumbers[i] < sortedNumbers[j] }) {
		t.Error("IsSorted should return true for sorted slice")
	}

	if IsSorted(unsortedNumbers, func(i, j int) bool { return unsortedNumbers[i] < unsortedNumbers[j] }) {
		t.Error("IsSorted should return false for unsorted slice")
	}
}

func TestBinarySearch(t *testing.T) {
	sortedNumbers := []int{1, 3, 5, 7, 9}

	// 查找存在的元素
	index := BinarySearch(sortedNumbers, 5, func(a, b int) bool { return a < b })
	if index != 2 {
		t.Errorf("BinarySearch failed for existing element. Got: %d, Want: 2", index)
	}

	// 查找不存在的元素
	index = BinarySearch(sortedNumbers, 6, func(a, b int) bool { return a < b })
	if index != 3 { // 应该插入到索引3的位置
		t.Errorf("BinarySearch failed for non-existing element. Got: %d, Want: 3", index)
	}
}
