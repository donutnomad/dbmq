package utils

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/types"
	"gorm.io/datatypes"
)

func Test000(t *testing.T) {
	dd := datatypes.NewJSONSlice([]types.PartitionInfo{
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
	consumers := []heartbeatrepo.HeartbeatPO{
		{ConsumerID: "consumer-c"},
		{ConsumerID: "consumer-a"},
		{ConsumerID: "consumer-b"},
	}

	SortConsumersByID(consumers)

	expected := []heartbeatrepo.HeartbeatPO{
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

func TestFindRevokedPartitions(t *testing.T) {
	tests := []struct {
		name          string
		oldPartitions []types.PartitionInfo
		newPartitions []types.PartitionInfo
		wantRevoked   []types.PartitionInfo
	}{
		{
			name:          "空旧分区",
			oldPartitions: nil,
			newPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}},
			wantRevoked:   nil,
		},
		{
			name:          "空新分区，全部撤销",
			oldPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
			newPartitions: nil,
			wantRevoked:   []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
		},
		{
			name:          "部分撤销",
			oldPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}, {Topic: "t1", Partition: 2}},
			newPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 1}},
			wantRevoked:   []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 2}},
		},
		{
			name:          "无撤销",
			oldPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}},
			newPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t1", Partition: 1}},
			wantRevoked:   nil,
		},
		{
			name:          "完全相同",
			oldPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 1}},
			newPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 1}},
			wantRevoked:   nil,
		},
		{
			name:          "跨Topic撤销",
			oldPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}, {Topic: "t2", Partition: 0}},
			newPartitions: []types.PartitionInfo{{Topic: "t1", Partition: 0}},
			wantRevoked:   []types.PartitionInfo{{Topic: "t2", Partition: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Subtract(tt.oldPartitions, tt.newPartitions)

			// 排序以便比较（因为 map 迭代顺序不确定）
			SortPartitionsByTopicAndPartition(got)
			SortPartitionsByTopicAndPartition(tt.wantRevoked)

			if !reflect.DeepEqual(got, tt.wantRevoked) {
				t.Errorf("findRevokedPartitions() = %v, want %v", got, tt.wantRevoked)
			}
		})
	}
}

func TestSameElements(t *testing.T) {
	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}
	tests := []struct {
		name string
		a, b []types.PartitionInfo
		want bool
	}{
		{"both nil", nil, nil, true},
		{"nil vs empty", nil, []types.PartitionInfo{}, true},
		{"same order", []types.PartitionInfo{p("t", 0)}, []types.PartitionInfo{p("t", 0)}, true},
		{"different order", []types.PartitionInfo{p("t", 0), p("t", 1)}, []types.PartitionInfo{p("t", 1), p("t", 0)}, true},
		{"different length", []types.PartitionInfo{p("t", 0)}, nil, false},
		{"different element", []types.PartitionInfo{p("t", 0)}, []types.PartitionInfo{p("t", 1)}, false},
		{"duplicate counts differ", []types.PartitionInfo{p("t", 0), p("t", 0)}, []types.PartitionInfo{p("t", 0), p("t", 1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SameElements(tt.a, tt.b); got != tt.want {
				t.Errorf("SameElements(%v, %v) = %v, want %v", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
