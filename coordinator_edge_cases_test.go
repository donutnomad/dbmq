package dbmq

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCoordinatorRebalanceDeadlock 测试协调器重新均衡死锁BUG
// BUG描述：rebalanceIfNeeded方法中存在潜在的死锁风险
// 当多个协调器实例同时处理同一个消费组时，可能导致数据库锁等待
func TestCoordinatorRebalanceDeadlock(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-deadlock-topic",
		PartitionCount: 2,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	// 创建两个协调器实例（模拟多实例部署）
	coordinator1 := NewCoordinator(CoordinatorConfig{
		DB:                db,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
		RebalanceTimeout:  5 * time.Second,
	})
	coordinator2 := NewCoordinator(CoordinatorConfig{
		DB:                db,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
		RebalanceTimeout:  5 * time.Second,
	})

	// 创建测试消费者
	groupID := "test-deadlock-group"
	consumer1 := types.ConsumerHeartbeat{
		ConsumerID:       "consumer-1",
		GroupID:          groupID,
		SubscribedTopics: []string{"test-deadlock-topic"},
		LastHeartbeat:    time.Now(),
	}
	consumer2 := types.ConsumerHeartbeat{
		ConsumerID:       "consumer-2",
		GroupID:          groupID,
		SubscribedTopics: []string{"test-deadlock-topic"},
		LastHeartbeat:    time.Now(),
	}

	// 插入消费者心跳
	require.NoError(t, db.Create(&consumer1).Error)
	require.NoError(t, db.Create(&consumer2).Error)

	// 并发执行重新均衡，测试死锁
	var wg sync.WaitGroup
	var errors []error
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if err := coordinator1.rebalanceIfNeeded(groupID); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("coordinator1 iteration %d: %w", i, err))
				mu.Unlock()
			}
		}(i)

		go func(i int) {
			defer wg.Done()
			if err := coordinator2.rebalanceIfNeeded(groupID); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("coordinator2 iteration %d: %w", i, err))
				mu.Unlock()
			}
		}(i)
	}

	// 等待所有goroutine完成，设置超时防止死锁
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 正常完成
		mu.Lock()
		if len(errors) > 0 {
			t.Logf("发现错误（可能正常）: %v", errors)
		}
		mu.Unlock()
	case <-time.After(30 * time.Second):
		t.Fatal("💥 BUG确认：协调器重新均衡出现死锁，30秒内未完成")
	}
}

// TestCoordinatorGenerationRaceCondition 测试代际ID竞争条件BUG
// BUG描述：IncrementAndGetGenerationID操作与分区分配更新之间存在竞争条件
// 可能导致不同消费者看到不一致的代际ID
func TestCoordinatorGenerationRaceCondition(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 创建测试主题
	topic := &types.Topic{
		TopicName:      "test-generation-topic",
		PartitionCount: 1,
		CreatedAt:      time.Now(),
	}
	require.NoError(t, db.Create(topic).Error)

	coordinator := NewCoordinator(CoordinatorConfig{
		DB:               db,
		HeartbeatTimeout: 5 * time.Second,
		RebalanceTimeout: 10 * time.Second,
	})

	groupID := "test-generation-group"

	// 创建初始消费者
	consumer := types.ConsumerHeartbeat{
		ConsumerID:       "consumer-1",
		GroupID:          groupID,
		SubscribedTopics: ([]string{"test-generation-topic"}),
		LastHeartbeat:    time.Now(),
	}
	require.NoError(t, db.Create(&consumer).Error)

	// 执行初始重新均衡
	require.NoError(t, coordinator.rebalanceIfNeeded(groupID))

	// 获取初始代际ID
	var initialGeneration types.ConsumerGroupGeneration
	require.NoError(t, db.Where("group_id = ?", groupID).First(&initialGeneration).Error)

	// 并发添加新消费者并触发重新均衡
	var generationIDs []uint
	var mu sync.Mutex
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// 添加新消费者
			newConsumer := types.ConsumerHeartbeat{
				ConsumerID:       fmt.Sprintf("consumer-%d", i+2),
				GroupID:          groupID,
				SubscribedTopics: ([]string{"test-generation-topic"}),
				LastHeartbeat:    time.Now(),
			}
			db.Create(&newConsumer)

			// 触发重新均衡
			coordinator.rebalanceIfNeeded(groupID)

			// 读取当前代际ID
			var generation types.ConsumerGroupGeneration
			if err := db.Where("group_id = ?", groupID).First(&generation).Error; err == nil {
				mu.Lock()
				generationIDs = append(generationIDs, generation.GenerationID)
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// 检查是否存在代际ID不一致
	mu.Lock()
	defer mu.Unlock()

	if len(generationIDs) == 0 {
		t.Fatal("未能获取到任何代际ID")
	}

	// 代际ID应该是递增的，检查是否有重复或乱序
	seen := make(map[uint]bool)
	for _, genID := range generationIDs {
		if seen[genID] {
			t.Errorf("💥 BUG确认：发现重复的代际ID %d，存在竞争条件", genID)
		}
		seen[genID] = true

		if genID <= initialGeneration.GenerationID {
			t.Errorf("💥 BUG确认：代际ID %d 小于等于初始代际ID %d，存在竞争条件",
				genID, initialGeneration.GenerationID)
		}
	}
}

// TestCoordinatorAssignmentInconsistency 测试分区分配不一致BUG
// BUG描述：calculateAssignments方法在边界情况下可能产生不一致的分配
func TestCoordinatorAssignmentInconsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	coordinator := NewCoordinator(CoordinatorConfig{
		DB: db,
	})

	// 测试用例1：空消费者列表
	assignments1 := coordinator.calculateAssignments([]types.ConsumerHeartbeat{}, []types.PartitionInfo{
		{Topic: "test-topic", Partition: 0},
		{Topic: "test-topic", Partition: 1},
	})
	assert.Empty(t, assignments1, "空消费者列表应该返回空分配")

	// 测试用例2：空分区列表
	consumers := []types.ConsumerHeartbeat{
		{ConsumerID: "consumer-1", GroupID: "test-group"},
		{ConsumerID: "consumer-2", GroupID: "test-group"},
	}
	assignments2 := coordinator.calculateAssignments(consumers, []types.PartitionInfo{})
	for consumerID, partitions := range assignments2 {
		assert.Empty(t, partitions, "消费者 %s 不应该分配到任何分区", consumerID)
	}

	// 测试用例3：分区数量少于消费者数量
	partitions := []types.PartitionInfo{
		{Topic: "test-topic", Partition: 0},
	}
	consumers3 := []types.ConsumerHeartbeat{
		{ConsumerID: "consumer-1", GroupID: "test-group"},
		{ConsumerID: "consumer-2", GroupID: "test-group"},
		{ConsumerID: "consumer-3", GroupID: "test-group"},
	}
	assignments3 := coordinator.calculateAssignments(consumers3, partitions)

	// 检查分配结果
	totalAssigned := 0
	emptyConsumers := 0
	for consumerID, assignedPartitions := range assignments3 {
		totalAssigned += len(assignedPartitions)
		if len(assignedPartitions) == 0 {
			emptyConsumers++
		}
		// 每个消费者最多应该分配到1个分区
		assert.LessOrEqual(t, len(assignedPartitions), 1,
			"消费者 %s 分配的分区数量不应超过1", consumerID)
	}

	// 总分配的分区数应该等于实际分区数
	assert.Equal(t, 1, totalAssigned, "总分配分区数应该等于实际分区数")
	// 应该有2个消费者没有分配到分区
	assert.Equal(t, 2, emptyConsumers, "应该有2个消费者没有分配到分区")

	// 测试用例4：多轮分配的一致性
	partitions4 := []types.PartitionInfo{
		{Topic: "topic-a", Partition: 0},
		{Topic: "topic-a", Partition: 1},
		{Topic: "topic-b", Partition: 0},
		{Topic: "topic-b", Partition: 1},
		{Topic: "topic-b", Partition: 2},
	}
	consumers4 := []types.ConsumerHeartbeat{
		{ConsumerID: "consumer-1", GroupID: "test-group"},
		{ConsumerID: "consumer-2", GroupID: "test-group"},
		{ConsumerID: "consumer-3", GroupID: "test-group"},
	}

	// 多次执行相同的分配，结果应该一致
	firstAssignment := coordinator.calculateAssignments(consumers4, partitions4)
	for i := 0; i < 10; i++ {
		assignment := coordinator.calculateAssignments(consumers4, partitions4)
		for consumerID, partitions := range assignment {
			expectedPartitions := firstAssignment[consumerID]
			assert.Equal(t, expectedPartitions, partitions,
				"第 %d 次分配中消费者 %s 的分区分配不一致", i+1, consumerID)
		}
	}
}

// TestCoordinatorMembersCacheInconsistency 测试成员缓存不一致BUG
// BUG描述：updateMembers和isRebalanceNeeded之间可能存在缓存不一致
func TestCoordinatorMembersCacheInconsistency(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	coordinator := NewCoordinator(CoordinatorConfig{
		DB:               db,
		HeartbeatTimeout: 5 * time.Second,
	})

	groupID := "test-cache-group"

	// 测试用例1：初始状态检查
	activeConsumerIDs := map[string]struct{}{
		"consumer-1": {},
		"consumer-2": {},
	}

	// 首次检查应该需要重新均衡（从空组到有成员）
	assert.True(t, coordinator.isRebalanceNeeded(groupID, activeConsumerIDs),
		"首次出现活跃成员应该需要重新均衡")

	// 更新成员缓存
	coordinator.updateMembers(groupID, activeConsumerIDs)

	// 相同成员集合应该不需要重新均衡
	assert.False(t, coordinator.isRebalanceNeeded(groupID, activeConsumerIDs),
		"相同成员集合不应该需要重新均衡")

	// 测试用例2：成员变化检测
	newActiveConsumerIDs := map[string]struct{}{
		"consumer-1": {},
		"consumer-3": {}, // consumer-2 离开，consumer-3 加入
	}

	assert.True(t, coordinator.isRebalanceNeeded(groupID, newActiveConsumerIDs),
		"成员变化应该需要重新均衡")

	// 测试用例3：并发访问缓存
	var wg sync.WaitGroup
	var results []bool
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			testConsumerIDs := map[string]struct{}{
				fmt.Sprintf("consumer-%d", i): {},
			}

			result := coordinator.isRebalanceNeeded(groupID, testConsumerIDs)

			mu.Lock()
			results = append(results, result)
			mu.Unlock()

			// 更新成员缓存
			coordinator.updateMembers(groupID, testConsumerIDs)
		}(i)
	}

	wg.Wait()

	// 检查是否有不一致的结果
	mu.Lock()
	assert.Equal(t, 10, len(results), "应该收集到10个结果")
	mu.Unlock()
}

// TestCoordinatorResourceLeak 测试协调器资源泄漏BUG
// BUG描述：协调器的goroutine和资源可能没有正确清理
func TestCoordinatorResourceLeak(t *testing.T) {
	db, _ := setupIntegrationTest(t)

	// 记录初始的goroutine数量
	initialGoroutines := countGoroutines()

	// 创建和销毁多个协调器实例
	for i := 0; i < 5; i++ {
		coordinator := NewCoordinator(CoordinatorConfig{
			DB:                     db,
			HeartbeatTimeout:       2 * time.Second,
			RebalanceInterval:      1 * time.Second,
			RetentionCheckInterval: 5 * time.Second,
		})

		// 启动协调器
		coordinator.Start()

		// 等待一段时间让协调器工作
		time.Sleep(100 * time.Millisecond)

		// 停止协调器
		coordinator.Stop()

		// 验证协调器已停止
		assert.True(t, coordinator.IsStopped(), "协调器应该已停止")

		// 等待goroutine清理
		time.Sleep(100 * time.Millisecond)
	}

	// 强制垃圾回收
	runtime.GC()
	time.Sleep(500 * time.Millisecond)

	// 检查goroutine是否有泄漏
	finalGoroutines := countGoroutines()
	goroutineDiff := finalGoroutines - initialGoroutines

	// 允许一些合理的goroutine增长（比如测试框架的goroutine）
	if goroutineDiff > 10 {
		t.Errorf("💥 BUG确认：可能存在goroutine泄漏，增加了 %d 个goroutine", goroutineDiff)
		t.Logf("初始goroutine数量: %d, 最终数量: %d", initialGoroutines, finalGoroutines)
	}
}

// 辅助函数

func mustMarshalJSON(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}

func countGoroutines() int {
	return runtime.NumGoroutine()
}
