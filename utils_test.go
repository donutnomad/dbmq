package dbmq

import (
	"encoding/json"
	"fmt"
	"github.com/donutnomad/dbmq/internal/db"
	"gorm.io/datatypes"
	"reflect"
	"testing"
)

func Test000(t *testing.T) {
	dd := datatypes.NewJSONSlice([]db.PartitionInfo{
		//{
		//	Topic:     "aaa",
		//	Partition: 1,
		//},
	})
	marshal, err := json.Marshal(&dd)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(marshal))
}

func TestSortConsumersByID(t *testing.T) {
	consumers := []db.ConsumerHeartbeat{
		{ConsumerID: "consumer-c"},
		{ConsumerID: "consumer-a"},
		{ConsumerID: "consumer-b"},
	}

	SortConsumersByID(consumers)

	expected := []db.ConsumerHeartbeat{
		{ConsumerID: "consumer-a"},
		{ConsumerID: "consumer-b"},
		{ConsumerID: "consumer-c"},
	}

	if !reflect.DeepEqual(consumers, expected) {
		t.Errorf("SortConsumersByID failed. Got: %v, Want: %v", consumers, expected)
	}
}

func TestSortPartitionsByTopicAndPartition(t *testing.T) {
	partitions := []db.PartitionInfo{
		{Topic: "topic-b", Partition: 1},
		{Topic: "topic-a", Partition: 2},
		{Topic: "topic-b", Partition: 0},
		{Topic: "topic-a", Partition: 1},
	}

	SortPartitionsByTopicAndPartition(partitions)

	expected := []db.PartitionInfo{
		{Topic: "topic-a", Partition: 1},
		{Topic: "topic-a", Partition: 2},
		{Topic: "topic-b", Partition: 0},
		{Topic: "topic-b", Partition: 1},
	}

	if !reflect.DeepEqual(partitions, expected) {
		t.Errorf("SortPartitionsByTopicAndPartition failed. Got: %v, Want: %v", partitions, expected)
	}
}

func TestFindRevokedPartitions(t *testing.T) {
	tests := []struct {
		name          string
		oldPartitions []db.PartitionInfo
		newPartitions []db.PartitionInfo
		wantRevoked   []db.PartitionInfo
	}{
		{
			name:          "空旧分区",
			oldPartitions: nil,
			newPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}},
			wantRevoked:   nil,
		},
		{
			name:          "空新分区，全部撤销",
			oldPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
			newPartitions: nil,
			wantRevoked:   []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
		},
		{
			name:          "部分撤销",
			oldPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
			newPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 1}},
			wantRevoked:   []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 2}},
		},
		{
			name:          "无撤销",
			oldPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}},
			newPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
			wantRevoked:   nil,
		},
		{
			name:          "完全相同",
			oldPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 1}},
			newPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 1}},
			wantRevoked:   nil,
		},
		{
			name:          "跨Topic撤销",
			oldPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 0}},
			newPartitions: []db.PartitionInfo{{Topic: "t1", Partition: 0}},
			wantRevoked:   []db.PartitionInfo{{Topic: "t2", Partition: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := subtract(tt.oldPartitions, tt.newPartitions)

			// 排序以便比较（因为 map 迭代顺序不确定）
			SortPartitionsByTopicAndPartition(got)
			SortPartitionsByTopicAndPartition(tt.wantRevoked)

			if !reflect.DeepEqual(got, tt.wantRevoked) {
				t.Errorf("findRevokedPartitions() = %v, want %v", got, tt.wantRevoked)
			}
		})
	}
}
