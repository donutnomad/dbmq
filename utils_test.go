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
