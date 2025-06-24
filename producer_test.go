package dbmq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNextRoundRobinPartition(t *testing.T) {
	producer := &Producer{
		roundRobinCounters: sync.Map{},
	}
	topic := "test-topic"
	partitionCount := uint(3)

	// We expect the sequence 0, 1, 2, 0, 1, 2, ...
	expectedSequence := []uint{0, 1, 2, 0, 1, 2}

	for i, expected := range expectedSequence {
		t.Run(fmt.Sprintf("Call %d", i+1), func(t *testing.T) {
			partition := producer.nextRoundRobinPartition(topic, partitionCount)
			if partition != expected {
				t.Errorf("Expected partition %d, but got %d", expected, partition)
			}
		})
	}
}

func TestNextRoundRobinPartition_SinglePartition(t *testing.T) {
	producer := &Producer{
		roundRobinCounters: sync.Map{},
	}
	topic := "single-partition-topic"
	partitionCount := uint(1)

	for i := 0; i < 5; i++ {
		partition := producer.nextRoundRobinPartition(topic, partitionCount)
		if partition != 0 {
			t.Errorf("Expected partition 0 for a single-partition topic, but got %d", partition)
		}
	}
}

func TestHashPartition(t *testing.T) {
	producer := &Producer{}
	partitionCount := uint(10)

	key1 := []byte("test-key-1")
	key2 := []byte("test-key-2")
	key1_again := []byte("test-key-1")

	p1 := producer.hashPartition(key1, partitionCount)
	p2 := producer.hashPartition(key2, partitionCount)
	p1_again := producer.hashPartition(key1_again, partitionCount)

	if p1 != p1_again {
		t.Errorf("Hash partitioning is not deterministic. Same key produced different partitions: %d vs %d", p1, p1_again)
	}

	if p1 == p2 {
		// This is not a strict failure, as hash collisions can happen.
		// But for these specific simple keys, it's highly unlikely with fnv.
		t.Logf("Warning: Different keys produced the same partition (%d). This could be a hash collision.", p1)
	}

	if p1 >= partitionCount {
		t.Errorf("Hash partition %d is out of bounds for partition count %d", p1, partitionCount)
	}
}

func TestPartitionSelectionLogic(t *testing.T) {
	// This is more of an integration-style test for the partition selection logic within the producer
	producer := &Producer{
		roundRobinCounters: sync.Map{},
	}
	partitionCount := uint(10)

	// Test case 1: Message with a key should use hash partitioning
	key := []byte("some-key")
	expectedHashPartition := producer.hashPartition(key, partitionCount)
	// Temporarily set the counter to a different value to ensure it's not used
	producer.roundRobinCounters.Store("test-topic", &atomic.Uint32{})
	producer.roundRobinCounters.LoadOrStore("test-topic", &atomic.Uint32{})
	counter, _ := producer.roundRobinCounters.Load("test-topic")
	counter.(*atomic.Uint32).Store(5)

	// We don't have a real DB, so we can't test Send directly.
	// Instead, we can check the logic by calling the partition functions.
	// This test relies on the internal implementation, which is okay for this level of testing.

	// If key is present
	msgWithKey := &ProducerMessage{Key: key}
	if msgWithKey.Key != nil && len(msgWithKey.Key) > 0 {
		p := producer.hashPartition(msgWithKey.Key, partitionCount)
		if p != expectedHashPartition {
			t.Errorf("Message with key did not use hash partition correctly. Got %d, want %d", p, expectedHashPartition)
		}
	}

	// Test case 2: Message without a key should use round-robin
	msgWithoutKey := &ProducerMessage{}
	if msgWithoutKey.Key == nil || len(msgWithoutKey.Key) == 0 {
		p := producer.nextRoundRobinPartition("test-topic", partitionCount)
		// The counter was set to 5, Add(1) makes it 6. (6-1)%10 = 5.
		if p != 5 {
			t.Errorf("Message without key did not use round-robin correctly. Got %d, want %d", p, 5)
		}
	}
}
