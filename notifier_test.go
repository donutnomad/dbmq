package dbmq

import (
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifier_FakeNotifier_Subscribe(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	partitions := []types.PartitionInfo{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic1", Partition: 1},
		{Topic: "topic2", Partition: 0},
	}

	// 测试订阅
	err := n.Subscribe(partitions)
	require.NoError(t, err)

	// 验证订阅记录
	calls := n.GetSubscribeCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, partitions, calls[0])

	// 验证已订阅的分区
	subscribed := n.GetSubscribed()
	assert.Len(t, subscribed, 3)
}

func TestNotifier_FakeNotifier_Unsubscribe(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	partitions := []types.PartitionInfo{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic1", Partition: 1},
	}

	// 先订阅
	err := n.Subscribe(partitions)
	require.NoError(t, err)

	// 取消订阅部分分区
	unsubPartitions := []types.PartitionInfo{
		{Topic: "topic1", Partition: 0},
	}
	err = n.Unsubscribe(unsubPartitions)
	require.NoError(t, err)

	// 验证取消订阅记录
	unsubCalls := n.GetUnsubscribeCalls()
	require.Len(t, unsubCalls, 1)
	assert.Equal(t, unsubPartitions, unsubCalls[0])

	// 验证剩余订阅
	subscribed := n.GetSubscribed()
	assert.Len(t, subscribed, 1)
	assert.Equal(t, types.PartitionInfo{Topic: "topic1", Partition: 1}, subscribed[0])
}

func TestNotifier_FakeNotifier_Notify(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	partition := types.PartitionInfo{Topic: "test-topic", Partition: 0}

	// 发送通知
	ok := n.Notify(partition)
	assert.True(t, ok)

	// 接收通知
	select {
	case msg := <-n.NotifyCh():
		assert.Equal(t, partition, msg.Partition)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected notification but none received")
	}
}

func TestNotifier_FakeNotifier_NotifyMultiple(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	partitions := []types.PartitionInfo{
		{Topic: "topic1", Partition: 0},
		{Topic: "topic1", Partition: 1},
		{Topic: "topic2", Partition: 0},
	}

	// 发送多个通知
	for _, p := range partitions {
		ok := n.Notify(p)
		assert.True(t, ok)
	}

	// 接收所有通知
	received := make([]types.PartitionInfo, 0, len(partitions))
	for range len(partitions) {
		select {
		case msg := <-n.NotifyCh():
			received = append(received, msg.Partition)
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("expected %d notifications but only received %d", len(partitions), len(received))
		}
	}

	assert.ElementsMatch(t, partitions, received)
}

func TestNotifier_FakeNotifier_SetHealthy(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	// 默认健康
	assert.True(t, n.IsHealthy())

	// 设置为不健康
	n.SetHealthy(false)
	assert.False(t, n.IsHealthy())

	// 恢复健康
	n.SetHealthy(true)
	assert.True(t, n.IsHealthy())
}

func TestNotifier_FakeNotifier_Close(t *testing.T) {
	n := NewFakeNotifier()

	// 关闭前是健康的
	assert.True(t, n.IsHealthy())

	// 关闭
	err := n.Close()
	require.NoError(t, err)

	// 关闭后不健康
	assert.False(t, n.IsHealthy())

	// 订阅应该失败
	err = n.Subscribe([]types.PartitionInfo{{Topic: "test", Partition: 0}})
	assert.Error(t, err)

	// 通知应该失败
	ok := n.Notify(types.PartitionInfo{Topic: "test", Partition: 0})
	assert.False(t, ok)

	// 重复关闭应该没问题
	err = n.Close()
	require.NoError(t, err)
}

func TestNotifier_FakeNotifier_Reset(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	// 执行一些操作
	partitions := []types.PartitionInfo{
		{Topic: "topic1", Partition: 0},
	}
	_ = n.Subscribe(partitions)
	n.SetHealthy(false)

	// 重置
	n.Reset()

	// 验证状态已重置
	assert.True(t, n.IsHealthy())
	assert.Empty(t, n.GetSubscribed())
	assert.Empty(t, n.GetSubscribeCalls())
	assert.Empty(t, n.GetUnsubscribeCalls())
}

func TestNotifier_FakeNotifier_EmptyPartitions(t *testing.T) {
	n := NewFakeNotifier()
	defer n.Close()

	// 空分区列表订阅应该成功且不记录
	err := n.Subscribe(nil)
	require.NoError(t, err)
	assert.Empty(t, n.GetSubscribeCalls())

	err = n.Subscribe([]types.PartitionInfo{})
	require.NoError(t, err)
	assert.Empty(t, n.GetSubscribeCalls())

	// 空分区列表取消订阅应该成功且不记录
	err = n.Unsubscribe(nil)
	require.NoError(t, err)
	assert.Empty(t, n.GetUnsubscribeCalls())

	err = n.Unsubscribe([]types.PartitionInfo{})
	require.NoError(t, err)
	assert.Empty(t, n.GetUnsubscribeCalls())
}

func TestNotifier_ChannelConversion(t *testing.T) {
	testCases := []struct {
		name      string
		partition types.PartitionInfo
	}{
		{
			name:      "simple topic",
			partition: types.PartitionInfo{Topic: "test-topic", Partition: 0},
		},
		{
			name:      "topic with numbers",
			partition: types.PartitionInfo{Topic: "topic123", Partition: 5},
		},
		{
			name:      "topic with underscores",
			partition: types.PartitionInfo{Topic: "my_topic_name", Partition: 10},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 转换为 channel 名称
			channels := partitionToChannels([]types.PartitionInfo{tc.partition})
			require.Len(t, channels, 1)

			// 验证 channel 名称格式
			assert.Contains(t, channels[0], "mq_notify:")
			assert.Contains(t, channels[0], tc.partition.Topic)

			// 转换回分区信息
			parsed, err := channelToPartition(channels[0])
			require.NoError(t, err)
			assert.Equal(t, tc.partition.Topic, parsed.Topic)
			assert.Equal(t, tc.partition.Partition, parsed.Partition)
		})
	}
}

// TestChannelToPartition_Invalid 验证畸形频道名返回 error 而非静默接受。
func TestChannelToPartition_Invalid(t *testing.T) {
	invalid := []string{
		"mq_notify:topic:0",   // 旧格式（无 hash tag），升级后应拒绝
		"mq_notify:{topic:0",  // 缺少结尾 }
		"mq_notify:{topic-0}", // 缺少冒号分隔
		"mq_notify:{topic:x}", // partition 非数字
		"mq_notify:{:0}",      // topic 为空
		"other:{topic:0}",     // 前缀不匹配
	}
	for _, ch := range invalid {
		_, err := channelToPartition(ch)
		assert.Error(t, err, "应拒绝畸形频道: %s", ch)
	}
}

// TestNotify_ClusterHashTagConsistency 验证状态键与通知频道使用相同的 hash tag，
// 从而在 Redis Cluster 下 hash 到同一 slot（修复 CROSSSLOT 的关键不变量）。
func TestNotify_ClusterHashTagConsistency(t *testing.T) {
	cases := []struct {
		topic     string
		partition uint
		wantTag   string
	}{
		{"orders", 0, "{orders:0}"},
		{"events", 7, "{events:7}"},
		{"a:b:c", 3, "{a:b:c:3}"}, // topic 含冒号
	}
	for _, c := range cases {
		stateKey := notifyStateKey(c.topic, c.partition)
		channel := notifyChannel(c.topic, c.partition)

		// 两个 key 必须包含完全相同的 hash tag
		assert.Contains(t, stateKey, c.wantTag, "stateKey 缺少正确的 hash tag")
		assert.Contains(t, channel, c.wantTag, "channel 缺少正确的 hash tag")

		// 频道能正确往返解析（topic 含冒号也不丢失）
		parsed, err := channelToPartition(channel)
		require.NoError(t, err)
		assert.Equal(t, c.topic, parsed.Topic)
		assert.Equal(t, c.partition, parsed.Partition)
	}
}

func TestNotifier_Interface(t *testing.T) {
	// 确保 FakeNotifier 实现了 Notifier 接口
	var _ Notifier = (*FakeNotifier)(nil)
	var _ Notifier = (*RedisNotifier)(nil)
}
