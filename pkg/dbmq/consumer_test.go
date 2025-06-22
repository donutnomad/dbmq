package dbmq

import (
	"dbmq/pkg/dbmq/types"
	"reflect"
	"sort"
	"sync"
	"testing"
)

// Helper function to sort PartitionInfo slices for consistent comparison
func sortPartitions(partitions []types.PartitionInfo) {
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].Partition < partitions[j].Partition
	})
}

func TestFindRevokedPartitions(t *testing.T) {
	testCases := []struct {
		name           string
		current        map[string][]uint
		next           map[string][]uint
		expectedRevoke []types.PartitionInfo
	}{
		{
			name: "No change",
			current: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {2},
			},
			next: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {2},
			},
			expectedRevoke: []types.PartitionInfo{},
		},
		{
			name: "Partition removed from a topic",
			current: map[string][]uint{
				"topic-a": {0, 1, 2},
			},
			next: map[string][]uint{
				"topic-a": {0, 2},
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 1},
			},
		},
		{
			name: "Entire topic removed",
			current: map[string][]uint{
				"topic-a": {0, 1},
				"topic-b": {0},
			},
			next: map[string][]uint{
				"topic-a": {0, 1},
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-b", Partition: 0},
			},
		},
		{
			name: "All partitions revoked",
			current: map[string][]uint{
				"topic-a": {0, 1},
			},
			next: map[string][]uint{},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 0},
				{Topic: "topic-a", Partition: 1},
			},
		},
		{
			name:    "Start with empty assignment",
			current: map[string][]uint{},
			next: map[string][]uint{
				"topic-a": {0},
			},
			expectedRevoke: []types.PartitionInfo{},
		},
		{
			name: "Complex change",
			current: map[string][]uint{
				"topic-a": {0, 1, 2},
				"topic-b": {0, 1},
				"topic-c": {5},
			},
			next: map[string][]uint{
				"topic-a": {2, 3}, // 0, 1 revoked
				"topic-c": {5},    // no change
				"topic-d": {0},    // new topic
			},
			expectedRevoke: []types.PartitionInfo{
				{Topic: "topic-a", Partition: 0},
				{Topic: "topic-a", Partition: 1},
				{Topic: "topic-b", Partition: 0},
				{Topic: "topic-b", Partition: 1},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			consumer := &Consumer{
				mu:         sync.RWMutex{},
				assignment: tc.current,
			}

			revoked := consumer.findRevokedPartitions(tc.next)

			// Sort both slices for consistent comparison
			sortPartitions(revoked)
			sortPartitions(tc.expectedRevoke)

			// Handle nil vs empty slice issue for reflect.DeepEqual
			if len(revoked) == 0 && len(tc.expectedRevoke) == 0 {
				// If both are empty (one might be nil, one might be an empty slice),
				// we consider them equal for the purpose of this test.
				return
			}

			if !reflect.DeepEqual(revoked, tc.expectedRevoke) {
				t.Errorf("findRevokedPartitions() failed\nGot: %v\nWant: %v", revoked, tc.expectedRevoke)
			}
		})
	}
}
