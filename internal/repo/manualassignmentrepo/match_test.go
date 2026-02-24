package manualassignmentrepo

import (
	"testing"

	"github.com/donutnomad/dbmq/internal/types"
)

func TestMatchAssignments(t *testing.T) {
	p := func(topic string, partition uint) types.PartitionInfo {
		return types.PartitionInfo{Topic: topic, Partition: partition}
	}

	tests := []struct {
		name        string
		assignments []AssignmentPO
		consumerIDs []string
		want        map[string][]types.PartitionInfo
	}{
		{
			name:        "空 assignments 返回 nil",
			assignments: nil,
			consumerIDs: []string{"worker-1"},
			want:        nil,
		},
		{
			name: "空 consumerIDs 返回 nil",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-1", Topic: "t1", Partition: 0},
			},
			consumerIDs: nil,
			want:        nil,
		},
		{
			name: "精确匹配成功",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-1", Topic: "orders", Partition: 0},
			},
			consumerIDs: []string{"worker-1", "worker-2"},
			want: map[string][]types.PartitionInfo{
				"worker-1": {p("orders", 0)},
			},
		},
		{
			name: "精确匹配失败",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-1", Topic: "orders", Partition: 0},
			},
			consumerIDs: []string{"worker-2", "worker-3"},
			want:        nil,
		},
		{
			name: "前缀匹配成功",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-*", Topic: "orders", Partition: 0},
			},
			consumerIDs: []string{"worker-1", "worker-2", "other-1"},
			want: map[string][]types.PartitionInfo{
				"worker-1": {p("orders", 0)},
				"worker-2": {p("orders", 0)},
			},
		},
		{
			name: "前缀匹配 - MAC地址格式ConsumerID",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:*", Topic: "events", Partition: 1},
			},
			consumerIDs: []string{
				"ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:914c29b2-fa97-4fba-bcb7-67b8e5ab3ab5",
				"ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
				"otherhost:11:22:33:44:55:66:some-uuid",
			},
			want: map[string][]types.PartitionInfo{
				"ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:914c29b2-fa97-4fba-bcb7-67b8e5ab3ab5": {p("events", 1)},
				"ubuntudeMacBook-Pro.local:da:cc:09:69:e9:bd:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee": {p("events", 1)},
			},
		},
		{
			name: "前缀匹配失败",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-*", Topic: "orders", Partition: 0},
			},
			consumerIDs: []string{"other-1", "other-2"},
			want:        nil,
		},
		{
			name: "多条规则混合匹配",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker-1", Topic: "orders", Partition: 0},
				{ConsumerIDPattern: "worker-*", Topic: "orders", Partition: 1},
				{ConsumerIDPattern: "monitor", Topic: "events", Partition: 0},
			},
			consumerIDs: []string{"worker-1", "worker-2", "monitor", "unknown"},
			want: map[string][]types.PartitionInfo{
				"worker-1": {p("orders", 0), p("orders", 1)}, // 精确 + 前缀都命中
				"worker-2": {p("orders", 1)},                 // 仅前缀命中
				"monitor":  {p("events", 0)},                 // 精确命中
			},
		},
		{
			name: "仅含星号的pattern匹配所有consumerID",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "*", Topic: "broadcast", Partition: 0},
			},
			consumerIDs: []string{"a", "b", "c"},
			want: map[string][]types.PartitionInfo{
				"a": {p("broadcast", 0)},
				"b": {p("broadcast", 0)},
				"c": {p("broadcast", 0)},
			},
		},
		{
			name: "精确匹配不做前缀匹配",
			assignments: []AssignmentPO{
				{ConsumerIDPattern: "worker", Topic: "orders", Partition: 0},
			},
			consumerIDs: []string{"worker", "worker-1", "worker-abc"},
			want: map[string][]types.PartitionInfo{
				"worker": {p("orders", 0)},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchAssignments(tt.assignments, tt.consumerIDs)

			if tt.want == nil {
				if got != nil {
					t.Fatalf("期望 nil，实际得到 %v", got)
				}
				return
			}

			if len(got) != len(tt.want) {
				t.Fatalf("结果长度不匹配: 期望 %d，实际 %d\n期望: %v\n实际: %v", len(tt.want), len(got), tt.want, got)
			}

			for consumerID, wantPartitions := range tt.want {
				gotPartitions, ok := got[consumerID]
				if !ok {
					t.Errorf("缺少 consumerID=%q 的结果", consumerID)
					continue
				}
				if len(gotPartitions) != len(wantPartitions) {
					t.Errorf("consumerID=%q: 分区数不匹配，期望 %v，实际 %v", consumerID, wantPartitions, gotPartitions)
					continue
				}
				for i := range wantPartitions {
					if gotPartitions[i] != wantPartitions[i] {
						t.Errorf("consumerID=%q 第%d个分区不匹配: 期望 %v，实际 %v", consumerID, i, wantPartitions[i], gotPartitions[i])
					}
				}
			}
		})
	}
}
