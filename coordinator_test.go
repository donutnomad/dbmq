package dbmq

import (
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	"github.com/donutnomad/dbmq/internal/types"

	"reflect"
	"testing"
)

// Helper function to create a slice of consumer heartbeats for testing.
func makeTestConsumers(ids ...string) []*heartbeat.Heartbeat {
	consumers := make([]*heartbeat.Heartbeat, len(ids))
	for i, id := range ids {
		consumers[i] = &heartbeat.Heartbeat{ConsumerID: id}
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
		utils.SortPartitionsByTopicAndPartition(assignments[cid])
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
	firstRun := coord.calculateAssignments(consumers, partitions, nil)
	secondRun := coord.calculateAssignments(consumers, partitions, nil)

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
	initialAssignment := coord.calculateAssignments(consumers1, partitions, nil)
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
	newAssignment := coord.calculateAssignments(consumers2, partitions, nil)
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
	noConsumers := coord.calculateAssignments(makeTestConsumers(), makeTestPartitions("topic-1", 5), nil)
	if len(noConsumers) != 0 {
		t.Errorf("Expected empty assignment for no consumers, got %v", noConsumers)
	}

	// Test with no partitions
	noPartitions := coord.calculateAssignments(makeTestConsumers("consumer-a"), makeTestPartitions("topic-1", 0), nil)
	if len(noPartitions["consumer-a"]) != 0 {
		t.Errorf("Expected empty assignment for no partitions, got %v", noPartitions)
	}
}

// TestCalculateAssignments_ManualOnly tests the case where all consumers have manual partition assignments.
func TestCalculateAssignments_ManualOnly(t *testing.T) {
	coord := &Coordinator{}

	// Setup: 3 consumers with manual assignments
	consumers := makeTestConsumers("consumer-a", "consumer-b", "consumer-c")
	partitions := makeTestPartitions("topic-1", 6)

	// Manual assignments: each consumer gets specific partitions
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
		"consumer-b": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 3}},
		"consumer-c": {{Topic: "topic-1", Partition: 4}, {Topic: "topic-1", Partition: 5}},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// Verify each consumer gets exactly their manual assignment
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
		"consumer-b": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 3}},
		"consumer-c": {{Topic: "topic-1", Partition: 4}, {Topic: "topic-1", Partition: 5}},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Manual-only assignment failed.\nGot: %v\nWant: %v", result, expected)
	}
}

// TestCalculateAssignments_AutoOnly tests the case where no consumers have manual assignments (original behavior).
func TestCalculateAssignments_AutoOnly(t *testing.T) {
	coord := &Coordinator{}

	// Setup: 3 consumers without manual assignments
	consumers := makeTestConsumers("consumer-a", "consumer-b", "consumer-c")
	partitions := makeTestPartitions("topic-1", 6)

	// No manual assignments (nil)
	result := coord.calculateAssignments(consumers, partitions, nil)
	result = sortAssignments(result)

	// Expected round-robin distribution:
	// partition 0 -> consumer-a, partition 1 -> consumer-b, partition 2 -> consumer-c
	// partition 3 -> consumer-a, partition 4 -> consumer-b, partition 5 -> consumer-c
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 3}},
		"consumer-b": {{Topic: "topic-1", Partition: 1}, {Topic: "topic-1", Partition: 4}},
		"consumer-c": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 5}},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Auto-only assignment failed.\nGot: %v\nWant: %v", result, expected)
	}
}

// TestCalculateAssignments_Mixed tests the case where some consumers have manual assignments and others don't.
func TestCalculateAssignments_Mixed(t *testing.T) {
	coord := &Coordinator{}

	// Setup: 3 consumers, only consumer-a has manual assignment
	consumers := makeTestConsumers("consumer-a", "consumer-b", "consumer-c")
	partitions := makeTestPartitions("topic-1", 6)

	// consumer-a has manual assignment for partitions 0 and 1
	// consumer-b and consumer-c should get the remaining partitions via round-robin
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// consumer-a gets manual partitions (0, 1)
	// Remaining partitions (2, 3, 4, 5) are distributed to consumer-b and consumer-c via round-robin
	// partition 2 -> consumer-b, partition 3 -> consumer-c
	// partition 4 -> consumer-b, partition 5 -> consumer-c
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
		"consumer-b": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 4}},
		"consumer-c": {{Topic: "topic-1", Partition: 3}, {Topic: "topic-1", Partition: 5}},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Mixed assignment failed.\nGot: %v\nWant: %v", result, expected)
	}
}

// TestCalculateAssignments_ManualPartitionExcluded tests that manually assigned partitions
// are excluded from automatic distribution to other consumers.
func TestCalculateAssignments_ManualPartitionExcluded(t *testing.T) {
	coord := &Coordinator{}

	// Setup: 2 consumers, one with manual assignment
	consumers := makeTestConsumers("consumer-a", "consumer-b")
	partitions := makeTestPartitions("topic-1", 4)

	// consumer-a manually gets partitions 0 and 2
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 2}},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// consumer-a gets manual partitions (0, 2)
	// consumer-b should only get remaining partitions (1, 3), NOT partitions 0 or 2
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 2}},
		"consumer-b": {{Topic: "topic-1", Partition: 1}, {Topic: "topic-1", Partition: 3}},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Manual partition exclusion failed.\nGot: %v\nWant: %v", result, expected)
	}

	// Additional verification: ensure no partition appears in both assignments
	aPartitions := make(map[types.PartitionInfo]bool)
	for _, p := range result["consumer-a"] {
		aPartitions[p] = true
	}
	for _, p := range result["consumer-b"] {
		if aPartitions[p] {
			t.Errorf("Partition %v is assigned to both consumers", p)
		}
	}
}

// TestCalculateAssignments_ManualWithEmptySlice tests that an empty manual assignment slice
// means the consumer participates in automatic assignment.
func TestCalculateAssignments_ManualWithEmptySlice(t *testing.T) {
	coord := &Coordinator{}

	consumers := makeTestConsumers("consumer-a", "consumer-b")
	partitions := makeTestPartitions("topic-1", 4)

	// consumer-a has an empty manual assignment (should be treated as auto)
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// Both consumers should participate in round-robin
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 2}},
		"consumer-b": {{Topic: "topic-1", Partition: 1}, {Topic: "topic-1", Partition: 3}},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Empty manual slice should trigger auto assignment.\nGot: %v\nWant: %v", result, expected)
	}
}

// TestCalculateAssignments_ManualWithMultipleTopics tests manual assignment across multiple topics.
func TestCalculateAssignments_ManualWithMultipleTopics(t *testing.T) {
	coord := &Coordinator{}

	consumers := makeTestConsumers("consumer-a", "consumer-b")

	// Create partitions for two topics
	partitions := []types.PartitionInfo{
		{Topic: "topic-1", Partition: 0},
		{Topic: "topic-1", Partition: 1},
		{Topic: "topic-2", Partition: 0},
		{Topic: "topic-2", Partition: 1},
	}

	// consumer-a manually gets topic-1:0 and topic-2:0
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {
			{Topic: "topic-1", Partition: 0},
			{Topic: "topic-2", Partition: 0},
		},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// consumer-b should get the remaining partitions
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {
			{Topic: "topic-1", Partition: 0},
			{Topic: "topic-2", Partition: 0},
		},
		"consumer-b": {
			{Topic: "topic-1", Partition: 1},
			{Topic: "topic-2", Partition: 1},
		},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Multi-topic manual assignment failed.\nGot: %v\nWant: %v", result, expected)
	}
}

// TestCalculateAssignments_AllManualNoRemainingPartitions tests the case where
// all partitions are manually assigned, leaving nothing for auto-assignment.
func TestCalculateAssignments_AllManualNoRemainingPartitions(t *testing.T) {
	coord := &Coordinator{}

	consumers := makeTestConsumers("consumer-a", "consumer-b", "consumer-c")
	partitions := makeTestPartitions("topic-1", 4)

	// consumer-a and consumer-b get all partitions manually
	// consumer-c has no manual assignment but no partitions remain
	manualAssignments := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
		"consumer-b": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 3}},
	}

	result := coord.calculateAssignments(consumers, partitions, manualAssignments)
	result = sortAssignments(result)

	// consumer-c should have an empty assignment (no partitions left)
	expected := map[string][]types.PartitionInfo{
		"consumer-a": {{Topic: "topic-1", Partition: 0}, {Topic: "topic-1", Partition: 1}},
		"consumer-b": {{Topic: "topic-1", Partition: 2}, {Topic: "topic-1", Partition: 3}},
		"consumer-c": {},
	}

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("All-manual no remaining partitions failed.\nGot: %v\nWant: %v", result, expected)
	}
}
