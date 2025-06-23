package pkg

import (
	"github.com/donutnomad/dbmq/pkg/types"
	"reflect"
	"testing"
)

// Helper function to create a slice of consumer heartbeats for testing.
func makeTestConsumers(ids ...string) []types.ConsumerHeartbeat {
	consumers := make([]types.ConsumerHeartbeat, len(ids))
	for i, id := range ids {
		consumers[i] = types.ConsumerHeartbeat{ConsumerID: id}
	}
	return consumers
}

// Helper function to create a slice of partition infos for testing.
func makeTestPartitions(topic string, count uint) []types.PartitionInfo {
	partitions := make([]types.PartitionInfo, count)
	for i := uint(0); i < count; i++ {
		partitions[i] = types.PartitionInfo{Topic: topic, Partition: i}
	}
	return partitions
}

// Helper function to sort the results for consistent comparison.
func sortAssignments(assignments map[string][]types.PartitionInfo) map[string][]types.PartitionInfo {
	for cid := range assignments {
		SortPartitionsByTopicAndPartition(assignments[cid])
	}
	return assignments
}

func TestCalculateAssignments_IsDeterministic(t *testing.T) {
	// Setup: Define a set of consumers and partitions
	consumers := makeTestConsumers("consumer-c", "consumer-a", "consumer-b")
	partitions := makeTestPartitions("topic-1", 5)

	// Create a dummy coordinator instance to call the method
	coord := &Coordinator{}

	// Run the assignment calculation multiple times
	firstRun := coord.calculateAssignments(consumers, partitions)
	secondRun := coord.calculateAssignments(consumers, partitions)

	// Sort the results to ensure deep equality check is consistent
	firstRun = sortAssignments(firstRun)
	secondRun = sortAssignments(secondRun)

	if !reflect.DeepEqual(firstRun, secondRun) {
		t.Errorf("Assignment calculation is not deterministic!\nFirst run: %v\nSecond run: %v", firstRun, secondRun)
	}
}

func TestCalculateAssignments_Stability(t *testing.T) {
	// Setup: Initial state with 3 consumers
	consumers1 := makeTestConsumers("consumer-a", "consumer-b", "consumer-c")
	partitions := makeTestPartitions("topic-1", 6)

	coord := &Coordinator{}

	// Calculate initial assignment
	initialAssignment := coord.calculateAssignments(consumers1, partitions)
	initialAssignment = sortAssignments(initialAssignment)

	// Expected initial assignment:
	// a: [topic-1:0, topic-1:3]
	// b: [topic-1:1, topic-1:4]
	// c: [topic-1:2, topic-1:5]
	expectedInitial := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 3}},
		"consumer-b": {{Topic: "topic-1", Partition: 1}, {Topic: "topic-1", Partition: 4}},
		"consumer-c": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 5}},
	}
	if !reflect.DeepEqual(initialAssignment, expectedInitial) {
		t.Errorf("Initial assignment was not as expected.\nGot: %v\nWant: %v", initialAssignment, expectedInitial)
	}

	// Scenario: One consumer leaves ("consumer-c")
	consumers2 := makeTestConsumers("consumer-a", "consumer-b")
	newAssignment := coord.calculateAssignments(consumers2, partitions)
	newAssignment = sortAssignments(newAssignment)

	// The 2 partitions from consumer-c should be redistributed to a and b.
	// a gets partition 2, b gets partition 5
	expectedNew := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 4}},
		"consumer-b": {{Topic: "topic-1", Partition: 1}, {Topic: "topic-1", Partition: 3}, {Topic: "topic-1", Partition: 5}},
	}

	// Verify that the new assignment is as expected and stable
	if !reflect.DeepEqual(newAssignment, expectedNew) {
		t.Errorf("Assignment after consumer loss was not stable or correct.\nGot: %v\nWant: %v", newAssignment, expectedNew)
	}

	// Verify that consumer-a and consumer-b kept their original partitions as much as possible
	if newAssignment["consumer-a"][0].Partition != 0 || newAssignment["consumer-b"][0].Partition != 1 {
		t.Errorf("Assignment was not stable. consumer-a or consumer-b lost original partitions without need.")
	}
}

func TestCalculateAssignments_EmptyInputs(t *testing.T) {
	coord := &Coordinator{}

	// Test with no consumers
	noConsumers := coord.calculateAssignments(makeTestConsumers(), makeTestPartitions("topic-1", 5))
	if len(noConsumers) != 0 {
		t.Errorf("Expected empty assignment for no consumers, got %v", noConsumers)
	}

	// Test with no partitions
	noPartitions := coord.calculateAssignments(makeTestConsumers("consumer-a"), makeTestPartitions("topic-1", 0))
	if len(noPartitions["consumer-a"]) != 0 {
		t.Errorf("Expected empty assignment for no partitions, got %v", noPartitions)
	}
}
