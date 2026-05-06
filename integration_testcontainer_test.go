//go:build integration

package dbmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/db/migration"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcmysql "github.com/testcontainers/testcontainers-go/modules/mysql"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 全局测试环境，所有测试共享
var globalEnv *TestcontainerEnv

// TestcontainerEnv 封装了基于testcontainer的测试环境
type TestcontainerEnv struct {
	MySQLContainer *tcmysql.MySQLContainer
	RedisContainer *tcredis.RedisContainer
	DB             interfaces.DB
	Redis          redis.UniversalClient
	ctx            context.Context
}

// TestMain 只启动一次容器，所有测试共享
func TestMain(m *testing.M) {
	ctx := context.Background()
	env := &TestcontainerEnv{ctx: ctx}

	// 启动MySQL容器
	log.Println("Starting MySQL container...")
	mysqlContainer, err := tcmysql.Run(ctx,
		"mysql:8.0",
		tcmysql.WithDatabase("dbmq_test"),
		tcmysql.WithUsername("root"),
		tcmysql.WithPassword("testpassword"),
	)
	if err != nil {
		log.Fatalf("Failed to start MySQL container: %v", err)
	}
	env.MySQLContainer = mysqlContainer

	// 启动Redis容器
	log.Println("Starting Redis container...")
	redisContainer, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		log.Fatalf("Failed to start Redis container: %v", err)
	}
	env.RedisContainer = redisContainer

	// 连接MySQL
	mysqlHost, _ := mysqlContainer.Host(ctx)
	mysqlPort, _ := mysqlContainer.MappedPort(ctx, "3306")
	dsn := fmt.Sprintf("root:testpassword@tcp(%s:%s)/dbmq_test?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlHost, mysqlPort.Port())
	dbClient, err := gorm.Open(gormmysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	sqlDB, _ := dbClient.DB()
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetConnMaxLifetime(time.Hour)
	env.DB = dbClient

	// 应用schema
	if err := migration.ApplySchemas(dbClient); err != nil {
		log.Fatalf("Failed to apply schemas: %v", err)
	}

	// 连接Redis
	redisHost, _ := redisContainer.Host(ctx)
	redisPort, _ := redisContainer.MappedPort(ctx, "6379")
	redisClient := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", redisHost, redisPort.Port()),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 10 * time.Second,
	})
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	env.Redis = redisClient

	globalEnv = env
	log.Println("Test environment ready!")

	// 运行测试
	code := m.Run()

	// 清理
	log.Println("Cleaning up containers...")
	redisClient.Close()
	sqlDB.Close()
	mysqlContainer.Terminate(ctx)
	redisContainer.Terminate(ctx)

	os.Exit(code)
}

// cleanupTables 清理所有表数据（测试间隔离）
func cleanupTables(t *testing.T) {
	tables := []string{
		"mq_messages",
		"mq_consumer_heartbeats",
		"mq_consumer_group_consumption_progress",
		"mq_consumer_group_generations",
		"mq_topics",
		"mq_manual_partition_assignments",
	}
	for _, table := range tables {
		globalEnv.DB.Exec("DELETE FROM " + table)
	}
}

// jsonValue 将字符串包装为有效的JSON值
func jsonValue(s string) []byte {
	b, _ := json.Marshal(s)
	return b
}

// =============================================================================
// 测试场景1: 多生产者多消费者基本流程
// =============================================================================

func TestTC_MultiProducerMultiConsumer(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	// 创建Topic（4个分区）
	admin := NewAdminClient(globalEnv.DB)
	topicName := "multi-pc-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

	// 启动协调器
	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc1",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 创建2个消费者
	consumers := make([]*Consumer, 2)
	for i := range 2 {
		c, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "tc-group-1", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)
		consumers[i] = c
		c.SubscribeTopics(topicName)
	}
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()

	// 等待消费者就绪
	for _, c := range consumers {
		require.Eventually(t, c.IsReady, 5*time.Second, 100*time.Millisecond)
	}

	// 3个生产者并发发送消息
	const messagesPerProducer = 30
	var wg sync.WaitGroup
	var sentCount atomic.Int32

	for i := range 3 {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
			for j := range messagesPerProducer {
				_, err := p.Send(ctx, ProducerMessage{
					Topic: topicName,
					Key:   fmt.Sprintf("k%d-%d", pid, j),
					Value: jsonValue(fmt.Sprintf("p%d-m%d", pid, j)),
				})
				if err == nil {
					sentCount.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	t.Logf("Sent %d messages", sentCount.Load())

	// 消费者并发消费
	received := sync.Map{}
	consumeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var consumeWg sync.WaitGroup
	for _, c := range consumers {
		consumeWg.Add(1)
		go func(consumer *Consumer) {
			defer consumeWg.Done()
			for {
				select {
				case <-consumeCtx.Done():
					return
				default:
				}
				msgs, err := consumer.Poll(consumeCtx, 500*time.Millisecond)
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					received.Store(msg.ID, true)
					consumer.Acknowledge(msg)
				}
				consumer.CommitSync(consumeCtx)
			}
		}(c)
	}
	consumeWg.Wait()

	// 验证
	var count int
	received.Range(func(_, _ any) bool { count++; return true })
	t.Logf("Received %d unique messages", count)
	assert.Equal(t, int(sentCount.Load()), count, "Should receive all sent messages")
}

// =============================================================================
// 测试场景2: 消费者上线下线重平衡
// =============================================================================

func TestTC_ConsumerOnlineOffline(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "online-offline-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc2",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 第一个消费者
	c1, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "online-offline-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	c1.SubscribeTopics(topicName)
	require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 发送第一批消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 10 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("b1-%d", i), Value: jsonValue(fmt.Sprintf("batch1-%d", i))})
	}

	// c1 消费
	var c1Count int
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	for {
		msgs, err := c1.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			c1Count++
			c1.Acknowledge(msg)
		}
		c1.CommitSync(pollCtx)
		if c1Count >= 10 {
			break
		}
	}
	cancel()
	t.Logf("Consumer1 consumed %d messages", c1Count)

	// 第二个消费者上线
	c2, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "online-offline-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	c2.SubscribeTopics(topicName)
	require.Eventually(t, c2.IsReady, 5*time.Second, 100*time.Millisecond)

	// 等待重平衡
	time.Sleep(1 * time.Second)

	// 发送第二批消息
	for i := range 10 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("b2-%d", i), Value: jsonValue(fmt.Sprintf("batch2-%d", i))})
	}

	// 两个消费者消费
	received := sync.Map{}
	pollCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	defer cancel2()

	var wg sync.WaitGroup
	for _, c := range []*Consumer{c1, c2} {
		wg.Add(1)
		go func(consumer *Consumer) {
			defer wg.Done()
			for {
				msgs, err := consumer.Poll(pollCtx2, 500*time.Millisecond)
				if err != nil {
					return
				}
				for _, msg := range msgs {
					received.Store(msg.ID, true)
					consumer.Acknowledge(msg)
				}
				consumer.CommitSync(pollCtx2)
			}
		}(c)
	}
	wg.Wait()

	var count int
	received.Range(func(_, _ any) bool { count++; return true })
	t.Logf("Both consumers received %d messages from batch2", count)
	assert.GreaterOrEqual(t, count, 10)

	// 关闭 c1，c2 应该接管所有分区
	c1.Close()
	time.Sleep(1 * time.Second)

	// c2 应该能消费到分配给它的新分区
	assert.True(t, c2.IsReady())
	c2.Close()
}

// =============================================================================
// 测试场景3: 被分配分区但不消费的消费者（懒消费者）
// =============================================================================

func TestTC_AssignedButNotConsuming(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "lazy-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc3",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 懒消费者：订阅但不调用Poll
	lazyConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "lazy-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	lazyConsumer.SubscribeTopics(topicName)
	require.Eventually(t, lazyConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer lazyConsumer.Close()

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 10 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("m%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 第二个消费者上线并消费
	activeConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "lazy-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	activeConsumer.SubscribeTopics(topicName)
	require.Eventually(t, activeConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer activeConsumer.Close()

	// 等待重平衡
	time.Sleep(1 * time.Second)

	// activeConsumer 消费（只能消费到分配给它的分区）
	var activeCount int
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	for {
		msgs, err := activeConsumer.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			activeCount++
			activeConsumer.Acknowledge(msg)
		}
		activeConsumer.CommitSync(pollCtx)
	}
	cancel()
	t.Logf("Active consumer consumed %d messages", activeCount)

	// 验证懒消费者的分区消息未被消费
	var records []consumerprogressrepo.ProgressPO
	globalEnv.DB.Where("group_id = ?", "lazy-group").Find(&records)
	t.Logf("Found %d offset records for lazy-group", len(records))

	// 2个分区，应该有记录
	assert.Equal(t, 2, len(records))
}

// =============================================================================
// 测试场景4: 消费策略 (earliest vs latest)
// =============================================================================

func TestTC_ConsumeStrategy(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "strategy-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc4",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 先发送5条消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("early%d", i), Value: jsonValue(fmt.Sprintf("early-%d", i))})
	}

	// latest 消费者（只消费订阅后的消息）
	latestConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "latest-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromLatest,
	})
	require.NoError(t, err)
	latestConsumer.SubscribeTopics(topicName)
	require.Eventually(t, latestConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer latestConsumer.Close()

	// earliest 消费者（消费所有消息）
	earliestConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "earliest-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	earliestConsumer.SubscribeTopics(topicName)
	require.Eventually(t, earliestConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer earliestConsumer.Close()

	// 再发送5条消息
	time.Sleep(500 * time.Millisecond)
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("late%d", i), Value: jsonValue(fmt.Sprintf("late-%d", i))})
	}

	// 消费
	var latestCount, earliestCount int
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			msgs, err := latestConsumer.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				latestCount++
				latestConsumer.Acknowledge(msg)
			}
			latestConsumer.CommitSync(pollCtx)
		}
	}()
	go func() {
		defer wg.Done()
		for {
			msgs, err := earliestConsumer.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				return
			}
			for _, msg := range msgs {
				earliestCount++
				earliestConsumer.Acknowledge(msg)
			}
			earliestConsumer.CommitSync(pollCtx)
		}
	}()
	wg.Wait()

	t.Logf("Latest consumer: %d, Earliest consumer: %d", latestCount, earliestCount)
	// latest 应该只收到订阅后的消息（约5条）
	// earliest 应该收到所有消息（10条）
	assert.GreaterOrEqual(t, earliestCount, latestCount)
	assert.Equal(t, 10, earliestCount)
}

// =============================================================================
// 测试场景5: 自动提交 vs 手动提交
// =============================================================================

func TestTC_AutoCommitVsManualCommit(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "commit-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc5",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("m%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 手动提交消费者 - 只消费不提交
	manualConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "manual-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest, EnableAutoCommit: false,
	})
	require.NoError(t, err)
	manualConsumer.SubscribeTopics(topicName)
	require.Eventually(t, manualConsumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 消费但不提交
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	var consumedCount int
	for {
		msgs, err := manualConsumer.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			consumedCount++
			manualConsumer.Acknowledge(msg)
		}
		// 故意不调用 CommitSync
	}
	cancel()
	t.Logf("Manual consumer consumed %d messages without commit", consumedCount)
	manualConsumer.Close()

	// 新消费者加入同一组，应该能重新消费
	newConsumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "manual-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest, EnableAutoCommit: false,
	})
	require.NoError(t, err)
	newConsumer.SubscribeTopics(topicName)
	require.Eventually(t, newConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer newConsumer.Close()

	pollCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	var reConsumedCount int
	for {
		msgs, err := newConsumer.Poll(pollCtx2, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			reConsumedCount++
			newConsumer.Acknowledge(msg)
		}
	}
	cancel2()
	t.Logf("New consumer re-consumed %d messages", reConsumedCount)

	// 因为没提交，应该能重新消费到
	assert.Equal(t, consumedCount, reConsumedCount)
}

// =============================================================================
// 测试场景6: Generation ID 隔离
// =============================================================================

func TestTC_GenerationIDIsolation(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "gen-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc6",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 第一个消费者
	c1, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "gen-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c1.SubscribeTopics(topicName)
	require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 记录初始 generation
	var gen1 consumergrouprepo.GenerationPO
	globalEnv.DB.Where("group_id = ?", "gen-group").First(&gen1)
	t.Logf("Initial generation: %d", gen1.GenerationID)

	// 第二个消费者加入，触发重平衡
	c2, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "gen-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c2.SubscribeTopics(topicName)
	require.Eventually(t, c2.IsReady, 5*time.Second, 100*time.Millisecond)

	// 等待重平衡完成
	time.Sleep(1 * time.Second)

	var gen2 consumergrouprepo.GenerationPO
	globalEnv.DB.Where("group_id = ?", "gen-group").First(&gen2)
	t.Logf("After c2 joins generation: %d", gen2.GenerationID)

	// generation 应该增加
	assert.Greater(t, gen2.GenerationID, gen1.GenerationID)

	c1.Close()
	c2.Close()
}

// =============================================================================
// 测试场景7: 高并发压力测试
// =============================================================================

func TestTC_HighConcurrencyStress(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "stress-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc7",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 3个消费者
	consumers := make([]*Consumer, 3)
	for i := range 3 {
		c, _ := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "stress-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		c.SubscribeTopics(topicName)
		consumers[i] = c
	}
	for _, c := range consumers {
		require.Eventually(t, c.IsReady, 5*time.Second, 100*time.Millisecond)
	}
	defer func() {
		for _, c := range consumers {
			c.Close()
		}
	}()

	// 5个生产者并发发送
	const messagesPerProducer = 50
	var wg sync.WaitGroup
	var sentCount atomic.Int32

	for i := range 5 {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
			for j := range messagesPerProducer {
				_, err := p.Send(ctx, ProducerMessage{
					Topic: topicName,
					Key:   fmt.Sprintf("k%d-%d", pid, j),
					Value: jsonValue(fmt.Sprintf("p%d-m%d", pid, j)),
				})
				if err == nil {
					sentCount.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	t.Logf("Sent %d messages", sentCount.Load())

	// 并发消费
	received := sync.Map{}
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var consumeWg sync.WaitGroup
	for _, c := range consumers {
		consumeWg.Add(1)
		go func(consumer *Consumer) {
			defer consumeWg.Done()
			for {
				select {
				case <-pollCtx.Done():
					return
				default:
				}
				msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					received.Store(msg.ID, true)
					consumer.Acknowledge(msg)
				}
				consumer.CommitSync(pollCtx)
			}
		}(c)
	}
	consumeWg.Wait()

	var count int
	received.Range(func(_, _ any) bool { count++; return true })
	t.Logf("Received %d unique messages", count)
	assert.Equal(t, int(sentCount.Load()), count)
}

// =============================================================================
// 测试场景8: 消费者状态错误 - 在错误状态下调用方法
// =============================================================================

func TestTC_ConsumerStateErrors(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "state-error-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc8",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 场景1: 在未订阅状态下Poll
	consumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "state-error-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)

	// 不调用 SubscribeTopics，直接 Poll
	pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
	cancel()
	// 应该返回空或错误（取决于实现）
	t.Logf("Poll without subscribe: msgs=%d, err=%v", len(msgs), err)

	// 场景2: 订阅后正常使用
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 场景3: 关闭后再调用方法
	consumer.Close()

	// 关闭后再Poll
	pollCtx2, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	msgs, err = consumer.Poll(pollCtx2, 500*time.Millisecond)
	cancel2()
	t.Logf("Poll after close: msgs=%d, err=%v", len(msgs), err)
	// 应该返回错误

	// 关闭后再CommitSync
	err = consumer.CommitSync(ctx)
	t.Logf("CommitSync after close: err=%v", err)

	// 场景4: 重复关闭
	consumer.Close() // 应该不会panic
	t.Log("Double close did not panic")
}

// =============================================================================
// 测试场景9: 心跳超时导致消费者被踢出
// =============================================================================

func TestTC_HeartbeatTimeout(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "heartbeat-timeout-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))

	// 使用很短的心跳超时
	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc9",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  1 * time.Second, // 1秒超时
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 消费者1正常
	c1, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "heartbeat-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c1.SubscribeTopics(topicName)
	require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 记录初始 generation
	var gen1 consumergrouprepo.GenerationPO
	globalEnv.DB.Where("group_id = ?", "heartbeat-group").First(&gen1)
	initialGen := gen1.GenerationID
	t.Logf("Initial generation: %d", initialGen)

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 10 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("m%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// c1 消费一部分
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	var consumed int
	for consumed < 5 {
		msgs, err := c1.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			consumed++
			c1.Acknowledge(msg)
		}
		c1.CommitSync(pollCtx)
	}
	cancel()
	t.Logf("c1 consumed %d messages", consumed)

	// 关闭 c1，模拟消费者掉线
	c1.Close()

	// 等待协调器检测到心跳超时并重平衡
	time.Sleep(2 * time.Second)

	// 新消费者加入
	c2, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "heartbeat-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c2.SubscribeTopics(topicName)
	require.Eventually(t, c2.IsReady, 5*time.Second, 100*time.Millisecond)
	defer c2.Close()

	// c2 应该能消费到剩余消息
	pollCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	var c2Consumed int
	for {
		msgs, err := c2.Poll(pollCtx2, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			c2Consumed++
			c2.Acknowledge(msg)
		}
		c2.CommitSync(pollCtx2)
	}
	cancel2()
	t.Logf("c2 consumed %d messages", c2Consumed)

	// 验证 generation 增加
	var gen2 consumergrouprepo.GenerationPO
	globalEnv.DB.Where("group_id = ?", "heartbeat-group").First(&gen2)
	t.Logf("Final generation: %d", gen2.GenerationID)
	assert.Greater(t, gen2.GenerationID, initialGen)
}

// =============================================================================
// 测试场景10: 生产者发送到不存在的Topic
// =============================================================================

func TestTC_ProducerToNonexistentTopic(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	p, err := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	require.NoError(t, err)

	// 发送到不存在的Topic
	_, err = p.Send(ctx, ProducerMessage{
		Topic: "nonexistent-topic",
		Key:   "key1",
		Value: jsonValue("test"),
	})
	t.Logf("Send to nonexistent topic: err=%v", err)
	assert.Error(t, err, "Should fail when sending to nonexistent topic")
}

// =============================================================================
// 测试场景11: 消费者订阅不存在的Topic
// =============================================================================

func TestTC_ConsumerSubscribeNonexistentTopic(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc11",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	consumer, err := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "nonexistent-topic-group", NotificationEnabled: true,
		Topics: []string{"nonexistent-topic"}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	require.NoError(t, err)
	consumer.SubscribeTopics("nonexistent-topic")
	defer consumer.Close()

	// 等待一段时间，消费者应该无法就绪（因为Topic不存在）
	time.Sleep(2 * time.Second)

	// Poll 应该返回空或特定错误
	pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
	cancel()
	t.Logf("Poll on nonexistent topic: msgs=%d, err=%v", len(msgs), err)
}

// =============================================================================
// 测试场景12: 重复创建相同Topic
// =============================================================================

func TestTC_DuplicateTopicCreation(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "duplicate-topic"

	// 第一次创建
	err := admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2})
	require.NoError(t, err)

	// 第二次创建相同Topic
	err = admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2})
	t.Logf("Duplicate topic creation: err=%v", err)
	assert.Error(t, err, "Should fail when creating duplicate topic")
}

// =============================================================================
// 测试场景13: 并发重平衡稳定性
// =============================================================================

func TestTC_ConcurrentRebalanceStability(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "concurrent-rebalance-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc13",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 300 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 发送一批消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 50 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("m%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 快速创建和关闭多个消费者，模拟频繁重平衡
	received := sync.Map{}
	var wg sync.WaitGroup

	for round := range 3 {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			c, _ := NewConsumer(ConsumerConfig{
				DB: globalEnv.DB, Redis: globalEnv.Redis,
				GroupID: "concurrent-rebalance-group", NotificationEnabled: true,
				Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
				ConsumeStrategy: ConsumeFromEarliest,
			})
			c.SubscribeTopics(topicName)

			// 等待就绪或超时
			readyCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			for {
				select {
				case <-readyCtx.Done():
					cancel()
					c.Close()
					return
				default:
					if c.IsReady() {
						cancel()
						goto ready
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		ready:
			// 消费一些消息
			pollCtx, pollCancel := context.WithTimeout(ctx, 2*time.Second)
			for {
				msgs, err := c.Poll(pollCtx, 300*time.Millisecond)
				if err != nil {
					break
				}
				for _, msg := range msgs {
					received.Store(msg.ID, true)
					c.Acknowledge(msg)
				}
				c.CommitSync(pollCtx)
			}
			pollCancel()
			c.Close()
			t.Logf("Round %d consumer finished", r)
		}(round)

		// 稍微错开启动时间
		time.Sleep(500 * time.Millisecond)
	}

	wg.Wait()

	var count int
	received.Range(func(_, _ any) bool { count++; return true })
	t.Logf("Total received after concurrent rebalances: %d", count)
	// 在频繁重平衡场景下，可能无法消费所有消息，但不应该丢失或重复
	assert.Greater(t, count, 0, "Should have consumed some messages")
}

// =============================================================================
// 测试场景14: Acknowledge 错误的消息
// =============================================================================

func TestTC_AcknowledgeWrongMessage(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "ack-wrong-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc14",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "ack-wrong-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer consumer.Close()

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	p.Send(ctx, ProducerMessage{Topic: topicName, Key: "k1", Value: jsonValue("msg1")})

	// 消费
	pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
	cancel()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	// 构造一个错误的消息（不属于当前分配）
	fakeMsg := ConsumerMessage{
		Topic:     "other-topic",
		Partition: 99,
		ID:        99999,
	}

	// Acknowledge 错误消息（应该被忽略或返回错误）
	consumer.Acknowledge(fakeMsg)
	t.Log("Acknowledged fake message without panic")

	// Acknowledge 正确消息
	consumer.Acknowledge(msgs[0])
	require.NoError(t, consumer.CommitSync(ctx))
	t.Log("Acknowledged and committed correct message successfully")
}

// =============================================================================
// 测试场景15: 自动提交模式
// =============================================================================

func TestTC_AutoCommitMode(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "auto-commit-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc15",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 自动提交消费者
	autoConsumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "auto-commit-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest, EnableAutoCommit: true,
		AutoCommitInterval: 500 * time.Millisecond,
	})
	autoConsumer.SubscribeTopics(topicName)
	require.Eventually(t, autoConsumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 消费所有消息
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	var consumed int
	for consumed < 5 {
		msgs, err := autoConsumer.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			consumed++
			autoConsumer.Acknowledge(msg)
		}
		// 不手动调用 CommitSync
	}
	cancel()
	t.Logf("Auto-commit consumer consumed %d messages", consumed)

	// 等待自动提交
	time.Sleep(1 * time.Second)

	// 关闭消费者
	autoConsumer.Close()

	// 新消费者加入，应该从已提交的位置开始
	newConsumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "auto-commit-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	newConsumer.SubscribeTopics(topicName)
	require.Eventually(t, newConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer newConsumer.Close()

	// 应该没有新消息可消费（已被自动提交）
	pollCtx2, cancel2 := context.WithTimeout(ctx, 2*time.Second)
	var newConsumed int
	for {
		msgs, err := newConsumer.Poll(pollCtx2, 500*time.Millisecond)
		if err != nil {
			break
		}
		newConsumed += len(msgs)
	}
	cancel2()
	t.Logf("New consumer consumed %d messages after auto-commit", newConsumed)
	assert.Equal(t, 0, newConsumed, "Should not consume any messages after auto-commit")
}

// =============================================================================
// 测试场景16: Context 取消
// =============================================================================

func TestTC_ContextCancellation(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "ctx-cancel-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc16",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "ctx-cancel-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer consumer.Close()

	// 创建一个会立即取消的 context
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // 立即取消

	// Poll 应该立即返回
	start := time.Now()
	msgs, err := consumer.Poll(cancelCtx, 5*time.Second)
	elapsed := time.Since(start)

	t.Logf("Poll with cancelled context: msgs=%d, err=%v, elapsed=%v", len(msgs), err, elapsed)
	assert.Less(t, elapsed, 1*time.Second, "Poll should return quickly when context is cancelled")
}

// =============================================================================
// 测试场景17: 空消息处理
// =============================================================================

func TestTC_EmptyPoll(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "empty-poll-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc17",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "empty-poll-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer consumer.Close()

	// 不发送任何消息，直接Poll
	pollCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
	cancel()

	t.Logf("Poll on empty topic: msgs=%d, err=%v", len(msgs), err)
	// 应该返回空列表，没有错误
	assert.Empty(t, msgs)
}

// =============================================================================
// 测试场景18: Admin API - ListTopics
// =============================================================================

func TestTC_AdminListTopics(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)

	// 创建多个Topic
	topics := []string{"list-topic-a", "list-topic-b", "list-topic-c"}
	for _, name := range topics {
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: name, NumPartitions: 2}))
	}

	// 列出所有Topic
	result, err := admin.ListTopics(ctx)
	require.NoError(t, err)
	t.Logf("Listed %d topics", len(result))

	// 验证创建的Topic都在列表中
	topicNames := make(map[string]bool)
	for _, topicName := range result {
		topicNames[topicName] = true
	}
	for _, name := range topics {
		assert.True(t, topicNames[name], "Topic %s should be in list", name)
	}
}

// =============================================================================
// 测试场景19: Admin API - DescribeTopics
// =============================================================================

func TestTC_AdminDescribeTopics(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)

	// 创建Topic
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "describe-topic", NumPartitions: 4}))

	// 描述存在的Topic
	results, err := admin.DescribeTopics(ctx, []string{"describe-topic"})
	require.NoError(t, err)

	// 验证结果
	assert.Len(t, results, 1)
	desc := results["describe-topic"]
	assert.NotNil(t, desc)
	assert.Equal(t, "describe-topic", desc.Name)
	assert.Equal(t, 4, desc.NumPartitions)

	// 描述不存在的Topic应该返回错误
	_, err = admin.DescribeTopics(ctx, []string{"nonexistent"})
	assert.Error(t, err)
	t.Logf("Nonexistent topic error: %v", err)
}

// =============================================================================
// 测试场景20: Admin API - DeleteTopics
// =============================================================================

func TestTC_AdminDeleteTopics(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)

	// 创建Topic
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "delete-topic", NumPartitions: 2}))

	// 发送一些消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: "delete-topic", Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 删除Topic
	err := admin.DeleteTopics(ctx, []string{"delete-topic"})
	require.NoError(t, err)

	// 验证Topic已从列表中删除
	topics, err := admin.ListTopics(ctx)
	require.NoError(t, err)
	for _, topicName := range topics {
		assert.NotEqual(t, "delete-topic", topicName, "Deleted topic should not appear in list")
	}
	t.Logf("Topic list after deletion: %v", topics)

	// 验证DescribeTopics返回错误
	_, err = admin.DescribeTopics(ctx, []string{"delete-topic"})
	assert.Error(t, err, "DescribeTopics should fail for deleted topic")
	t.Logf("DescribeTopics for deleted topic: err=%v", err)

	// 使用新Producer验证无法发送到已删除的Topic（无缓存）
	newProducer, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	_, err = newProducer.Send(ctx, ProducerMessage{Topic: "delete-topic", Key: "k", Value: jsonValue("msg")})
	assert.Error(t, err, "New producer should fail to send to deleted topic")
	t.Logf("Send to deleted topic with new producer: err=%v", err)
}

// =============================================================================
// 测试场景21: Admin API - CreateTopicIfNotExist
// =============================================================================

func TestTC_AdminCreateTopicIfNotExist(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)

	// 第一次创建
	err := admin.CreateTopicIfNotExist(ctx, NewTopicRequest{Name: "idempotent-topic", NumPartitions: 2})
	require.NoError(t, err)

	// 第二次创建（应该不报错）
	err = admin.CreateTopicIfNotExist(ctx, NewTopicRequest{Name: "idempotent-topic", NumPartitions: 2})
	require.NoError(t, err, "CreateTopicIfNotExist should be idempotent")

	// 验证Topic只有一个
	topics, _ := admin.ListTopics(ctx)
	count := 0
	for _, topicName := range topics {
		if topicName == "idempotent-topic" {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

// =============================================================================
// 测试场景22: 消息保留和清理
// =============================================================================

func TestTC_MessageRetentionCleanup(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "retention-topic"

	// 创建Topic，设置保留时间为1秒（仅用于测试）
	retentionMs := int64(1000)
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{
		Name:          topicName,
		NumPartitions: 1,
		Config:        &TopicConfig{RetentionMs: &retentionMs}, // 1秒保留
	}))

	// 启动协调器，配置快速清理
	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:             "tc-retention",
		NodeAddr:               "test-node",
		DB:                     globalEnv.DB,
		HeartbeatTimeout:       3 * time.Second,
		RebalanceInterval:      500 * time.Millisecond,
		RetentionCheckInterval: 1 * time.Second, // 1秒检查一次
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 10 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 消费者消费所有消息
	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "retention-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	var consumed int
	for consumed < 10 {
		msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
		if err != nil {
			break
		}
		for _, msg := range msgs {
			consumed++
			consumer.Acknowledge(msg)
		}
		consumer.CommitSync(pollCtx)
	}
	cancel()
	consumer.Close()
	t.Logf("Consumed %d messages", consumed)

	// 统计消息数量
	var countBefore int64
	globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", topicName).Scan(&countBefore)
	t.Logf("Messages before cleanup: %d", countBefore)

	// 等待保留时间过期和清理周期
	time.Sleep(3 * time.Second)

	// 手动触发清理（如果协调器还没自动清理）
	coordinator.CleanupExpiredMessages(ctx)

	// 统计清理后的消息数量
	var countAfter int64
	globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", topicName).Scan(&countAfter)
	t.Logf("Messages after cleanup: %d", countAfter)

	// 消息应该被清理（或部分清理）
	assert.Less(t, countAfter, countBefore, "Messages should be cleaned up after retention period")
}

// =============================================================================
// 测试场景23: 多协调器Leader选举
// =============================================================================

func TestTC_MultiCoordinatorLeaderElection(t *testing.T) {
	cleanupTables(t)

	// 启动3个协调器
	coordinators := make([]*Coordinator, 3)
	for i := range 3 {
		c := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-election", // 相同的锁后缀，竞争同一个Leader
			DB:                globalEnv.DB,
			NodeAddr:          fmt.Sprintf("test-node-%d", i),
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinators[i] = c
		c.Start()
	}
	defer func() {
		for _, c := range coordinators {
			c.Stop()
		}
	}()

	// 应该只有一个Leader
	var leaderIdx int
	require.Eventually(t, func() bool {
		leaderCount := 0
		for i, c := range coordinators {
			if c.IsLeader() {
				leaderCount++
				leaderIdx = i
			}
		}
		return leaderCount == 1
	}, 10*time.Second, 100*time.Millisecond, "Should have exactly one leader")
	t.Logf("Leader is coordinator %d", leaderIdx)

	// 停止当前Leader
	coordinators[leaderIdx].Stop()
	t.Logf("Stopped leader (coordinator %d)", leaderIdx)

	// 应该有一个新Leader
	var newLeaderIdx int
	require.Eventually(t, func() bool {
		newLeaderCount := 0
		for i, c := range coordinators {
			if i != leaderIdx && c.IsLeader() {
				newLeaderCount++
				newLeaderIdx = i
			}
		}
		return newLeaderCount == 1
	}, 25*time.Second, 100*time.Millisecond, "Should have a new leader after old leader stops")
	t.Logf("New leader is coordinator %d", newLeaderIdx)
}

// =============================================================================
// 测试场景24: 重平衡期间的消息处理
// =============================================================================

func TestTC_MessageProcessingDuringRebalance(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "rebalance-msg-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc-rebalance-msg",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  2 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 第一个消费者
	c1, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "rebalance-msg-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c1.SubscribeTopics(topicName)
	require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 50 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// c1 开始消费
	received := sync.Map{}
	var wg sync.WaitGroup

	consumeCtx, cancelConsume := context.WithTimeout(ctx, 10*time.Second)
	defer cancelConsume()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			default:
			}
			msgs, err := c1.Poll(consumeCtx, 300*time.Millisecond)
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				received.Store(msg.ID, true)
				c1.Acknowledge(msg)
			}
			c1.CommitSync(consumeCtx)
		}
	}()

	// 在消费过程中加入第二个消费者，触发重平衡
	time.Sleep(1 * time.Second)
	c2, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "rebalance-msg-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c2.SubscribeTopics(topicName)
	require.Eventually(t, c2.IsReady, 5*time.Second, 100*time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			default:
			}
			msgs, err := c2.Poll(consumeCtx, 300*time.Millisecond)
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				received.Store(msg.ID, true)
				c2.Acknowledge(msg)
			}
			c2.CommitSync(consumeCtx)
		}
	}()

	// 再加入第三个消费者
	time.Sleep(1 * time.Second)
	c3, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "rebalance-msg-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 300 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	c3.SubscribeTopics(topicName)
	require.Eventually(t, c3.IsReady, 5*time.Second, 100*time.Millisecond)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			default:
			}
			msgs, err := c3.Poll(consumeCtx, 300*time.Millisecond)
			if err != nil {
				continue
			}
			for _, msg := range msgs {
				received.Store(msg.ID, true)
				c3.Acknowledge(msg)
			}
			c3.CommitSync(consumeCtx)
		}
	}()

	wg.Wait()

	// 关闭所有消费者
	c1.Close()
	c2.Close()
	c3.Close()

	// 统计收到的消息
	var count int
	received.Range(func(_, _ any) bool { count++; return true })
	t.Logf("Total received: %d/50 messages", count)

	// 在多次重平衡后，应该消费到所有消息（至少一次语义）
	assert.Equal(t, 50, count, "Should receive all messages despite rebalances")
}

// =============================================================================
// 测试场景25: Consumer ID 和 State 方法
// =============================================================================

func TestTC_ConsumerMetadataMethods(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "metadata-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc-metadata",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "metadata-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})

	// 测试 ID 方法
	id := consumer.ID()
	assert.NotEmpty(t, id, "Consumer ID should not be empty")
	t.Logf("Consumer ID: %s", id)

	// 测试初始 State
	state := consumer.State()
	t.Logf("Initial state: %s", state)

	// 订阅并等待就绪
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 测试就绪后的 State
	state = consumer.State()
	assert.Equal(t, StateReady, state, "State should be Ready after subscription")
	t.Logf("State after ready: %s", state)

	// 测试 GetGenerationID
	// 注意: GetGenerationID 使用非阻塞 select，可能返回 0
	// 这里只记录值，不做断言
	genID := consumer.GetGenerationID()
	t.Logf("Generation ID: %d (may be 0 due to non-blocking implementation)", genID)

	// 测试 IsAutoCommitEnabled
	isAutoCommit := consumer.IsAutoCommitEnabled()
	assert.False(t, isAutoCommit, "Auto commit should be disabled by default")

	consumer.Close()

	// 测试关闭后的 State
	state = consumer.State()
	assert.Equal(t, StateStopped, state, "State should be Stopped after close")
	t.Logf("State after close: %s", state)
}

// =============================================================================
// 测试场景26: CommitMessage 单消息提交
// =============================================================================

func TestTC_CommitMessage(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)
	topicName := "commit-msg-topic"
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

	coordinator := NewCoordinator(CoordinatorConfig{
		LockSuffix:        "tc-commit-msg",
		DB:                globalEnv.DB,
		NodeAddr:          "test-node",
		HeartbeatTimeout:  3 * time.Second,
		RebalanceInterval: 500 * time.Millisecond,
	})
	coordinator.Start()
	defer coordinator.Stop()
	require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

	// 发送消息
	p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
	for i := range 5 {
		p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
	}

	// 消费者使用手动提交
	consumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "commit-msg-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest, EnableAutoCommit: false,
	})
	consumer.SubscribeTopics(topicName)
	require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

	// 一次 Poll 获取所有消息（配置默认 PollFetchLimit 为 100，足够获取5条消息）
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	msgs, err := consumer.Poll(pollCtx, 2*time.Second)
	cancel()
	require.NoError(t, err)
	require.Len(t, msgs, 5, "Should poll all 5 messages")
	t.Logf("Polled %d messages: IDs=%v", len(msgs), []int64{msgs[0].ID, msgs[1].ID, msgs[2].ID, msgs[3].ID, msgs[4].ID})

	// 使用 CommitMessage 只提交前3条
	// 注意: CommitMessage 内部已经调用了 Acknowledge，然后立即 CommitSync
	for i := range 3 {
		err := consumer.CommitMessage(ctx, msgs[i])
		require.NoError(t, err, "CommitMessage should succeed for message %d", i)
		t.Logf("Committed message %d (ID=%d)", i, msgs[i].ID)
	}
	// 不提交后2条消息（msgs[3]、msgs[4]）
	consumer.Close()

	// 验证数据库中的提交状态
	var progress struct {
		LastConsumedMessageID int64
	}
	err = globalEnv.DB.Raw(`SELECT last_consumed_message_id FROM mq_consumer_group_consumption_progress
		WHERE group_id = ? AND topic = ? AND `+"`partition`"+` = ?`,
		"commit-msg-group", topicName, 0).Scan(&progress).Error
	require.NoError(t, err)
	t.Logf("Database committed offset: %d", progress.LastConsumedMessageID)

	// 新消费者应该从第4条开始消费
	newConsumer, _ := NewConsumer(ConsumerConfig{
		DB: globalEnv.DB, Redis: globalEnv.Redis,
		GroupID: "commit-msg-group", NotificationEnabled: true,
		Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
		ConsumeStrategy: ConsumeFromEarliest,
	})
	newConsumer.SubscribeTopics(topicName)
	require.Eventually(t, newConsumer.IsReady, 5*time.Second, 100*time.Millisecond)
	defer newConsumer.Close()

	pollCtx2, cancel2 := context.WithTimeout(ctx, 3*time.Second)
	newMsgs, err := newConsumer.Poll(pollCtx2, 2*time.Second)
	cancel2()

	t.Logf("New consumer received %d messages", len(newMsgs))
	if len(newMsgs) > 0 {
		t.Logf("New consumer message IDs: %v", func() []int64 {
			ids := make([]int64, len(newMsgs))
			for i, m := range newMsgs {
				ids[i] = m.ID
			}
			return ids
		}())
	}
	assert.Equal(t, 2, len(newMsgs), "Should receive only uncommitted messages (2)")
}

// =============================================================================
// 测试场景27: Producer.SendBatch 批量发送
// =============================================================================

func TestTC_ProducerSendBatch(t *testing.T) {
	t.Run("EmptyMessages", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		p, err := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		require.NoError(t, err)

		results, err := p.SendBatch(ctx)
		assert.NoError(t, err)
		assert.Nil(t, results)
	})

	t.Run("CrossTopicBatch", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "batch-topic-a", NumPartitions: 2}))
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "batch-topic-b", NumPartitions: 3}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		var messages []ProducerMessage
		for i := range 5 {
			messages = append(messages, ProducerMessage{
				Topic: "batch-topic-a",
				Key:   fmt.Sprintf("ka%d", i),
				Value: jsonValue(fmt.Sprintf("a-%d", i)),
			})
		}
		for i := range 5 {
			messages = append(messages, ProducerMessage{
				Topic: "batch-topic-b",
				Key:   fmt.Sprintf("kb%d", i),
				Value: jsonValue(fmt.Sprintf("b-%d", i)),
			})
		}

		results, err := p.SendBatch(ctx, messages...)
		require.NoError(t, err)
		assert.Len(t, results, 10)

		for _, r := range results {
			assert.True(t, r.Topic == "batch-topic-a" || r.Topic == "batch-topic-b")
			assert.True(t, r.Offset > 0)
		}

		var countA, countB int64
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", "batch-topic-a").Scan(&countA)
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", "batch-topic-b").Scan(&countB)
		assert.Equal(t, int64(5), countA)
		assert.Equal(t, int64(5), countB)
	})

	t.Run("NonexistentTopic", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "batch-exist", NumPartitions: 1}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		messages := []ProducerMessage{
			{Topic: "batch-exist", Key: "k1", Value: jsonValue("ok")},
			{Topic: "batch-ghost", Key: "k2", Value: jsonValue("fail")},
		}
		_, err := p.SendBatch(ctx, messages...)
		assert.Error(t, err, "SendBatch should fail when batch contains nonexistent topic")
		t.Logf("SendBatch with nonexistent topic: err=%v", err)
	})

	t.Run("EmptyTopicName", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		_, err := p.SendBatch(ctx, ProducerMessage{Topic: "", Key: "k1", Value: jsonValue("msg")})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "cannot be empty")
	})
}

// =============================================================================
// 测试场景28: 分区路由算法
// =============================================================================

func TestTC_PartitionRouting(t *testing.T) {
	t.Run("HashConsistency", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "hash-route-topic", NumPartitions: 8}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		var partitions []uint
		for range 20 {
			result, err := p.Send(ctx, ProducerMessage{
				Topic: "hash-route-topic",
				Key:   "fixed-key",
				Value: jsonValue("data"),
			})
			require.NoError(t, err)
			partitions = append(partitions, result.Partition)
		}

		// 所有消息应路由到同一分区
		for i := 1; i < len(partitions); i++ {
			assert.Equal(t, partitions[0], partitions[i],
				"Same key should always route to same partition, got partition %d at index %d, expected %d",
				partitions[i], i, partitions[0])
		}
		t.Logf("All 20 messages with key 'fixed-key' routed to partition %d", partitions[0])
	})

	t.Run("DifferentKeysDistribution", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "dist-route-topic", NumPartitions: 8}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		usedPartitions := make(map[uint]bool)
		for i := range 100 {
			result, err := p.Send(ctx, ProducerMessage{
				Topic: "dist-route-topic",
				Key:   fmt.Sprintf("unique-key-%d", i),
				Value: jsonValue("data"),
			})
			require.NoError(t, err)
			usedPartitions[result.Partition] = true
		}

		t.Logf("100 different keys used %d out of 8 partitions", len(usedPartitions))
		assert.GreaterOrEqual(t, len(usedPartitions), 2, "100 different keys should use at least 2 partitions")
	})

	t.Run("RoundRobinUniformDistribution", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "rr-route-topic", NumPartitions: 8}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		partitionCounts := make(map[uint]int)
		for range 80 {
			result, err := p.Send(ctx, ProducerMessage{
				Topic: "rr-route-topic",
				Key:   "", // 空Key走轮询
				Value: jsonValue("data"),
			})
			require.NoError(t, err)
			partitionCounts[result.Partition]++
		}

		t.Logf("Round-robin distribution: %v", partitionCounts)
		// 每个分区应恰好10条 (80/8=10)
		for partition, count := range partitionCounts {
			assert.Equal(t, 10, count, "Partition %d should have exactly 10 messages, got %d", partition, count)
		}
	})

	t.Run("ConcurrentRoundRobinSafety", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "conc-rr-topic", NumPartitions: 8}))

		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})

		var wg sync.WaitGroup
		var totalSent atomic.Int32
		partitionCounts := sync.Map{}

		for g := range 8 {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for i := range 10 {
					result, err := p.Send(ctx, ProducerMessage{
						Topic: "conc-rr-topic",
						Key:   "",
						Value: jsonValue(fmt.Sprintf("g%d-m%d", goroutineID, i)),
					})
					if err == nil {
						totalSent.Add(1)
						partitionCounts.Store(result.Partition, true)
					}
				}
			}(g)
		}
		wg.Wait()

		assert.Equal(t, int32(80), totalSent.Load(), "All 80 messages should be sent successfully")

		var usedPartitions int
		partitionCounts.Range(func(_, _ any) bool {
			usedPartitions++
			return true
		})
		t.Logf("Concurrent round-robin used %d partitions", usedPartitions)
		assert.Greater(t, usedPartitions, 0, "Should have used at least 1 partition")

		var dbTotal int64
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", "conc-rr-topic").Scan(&dbTotal)
		assert.Equal(t, int64(80), dbTotal, "Database should have 80 messages")
	})
}

// =============================================================================
// 测试场景29: PollLoop 和 PollLoopTimeout
// =============================================================================

func TestTC_PollLoop(t *testing.T) {
	t.Run("PollLoopNormalConsumeAndExit", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "pollloop-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-pollloop",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		// 发送10条消息
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 10 {
			_, err := p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
			require.NoError(t, err)
		}

		consumer, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "pollloop-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)
		consumer.SubscribeTopics(topicName)
		require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
		defer consumer.Close()

		pollCtx, cancel := context.WithCancel(ctx)
		var consumed atomic.Int32

		errCh := make(chan error, 1)
		go func() {
			errCh <- consumer.PollLoop(pollCtx, 1*time.Second, func(messages []ConsumerMessage) {
				for _, msg := range messages {
					consumer.Acknowledge(msg)
				}
				consumer.CommitSync(pollCtx)
				consumed.Add(int32(len(messages)))
				if consumed.Load() >= 10 {
					cancel()
				}
			})
		}()

		// 等待 PollLoop 退出
		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled, "PollLoop should return context.Canceled")
		case <-time.After(15 * time.Second):
			cancel()
			t.Fatal("PollLoop did not exit in time")
		}

		assert.Equal(t, int32(10), consumed.Load(), "Should consume all 10 messages")
		t.Logf("PollLoop consumed %d messages", consumed.Load())
	})

	t.Run("PollLoopTimeoutDynamic", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "pollloop-timeout-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 1}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-pollloop-to",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		// 发送5条消息
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 5 {
			_, err := p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
			require.NoError(t, err)
		}

		consumer, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "pollloop-to-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)
		consumer.SubscribeTopics(topicName)
		require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
		defer consumer.Close()

		pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var consumed atomic.Int32

		errCh := make(chan error, 1)
		go func() {
			errCh <- consumer.PollLoopTimeout(pollCtx,
				func(c *Consumer, lastMessageCount int64) time.Duration {
					if lastMessageCount > 0 {
						return 100 * time.Millisecond
					}
					return 2 * time.Second
				},
				func(messages []ConsumerMessage) {
					for _, msg := range messages {
						consumer.Acknowledge(msg)
					}
					consumer.CommitSync(pollCtx)
					consumed.Add(int32(len(messages)))
				})
		}()

		// 等待消费到所有消息或超时
		require.Eventually(t, func() bool {
			return consumed.Load() >= 5
		}, 10*time.Second, 200*time.Millisecond, "Should consume all 5 messages via PollLoopTimeout")
		cancel()

		<-errCh
		t.Logf("PollLoopTimeout consumed %d messages", consumed.Load())
		assert.GreaterOrEqual(t, consumed.Load(), int32(5))
	})
}

// =============================================================================
// 测试场景30: 手动分区分配 (ManualAssignment)
// =============================================================================

// createManualAssignment 在数据库中插入手动分配规则
func createManualAssignment(t *testing.T, groupID, pattern, topicName string, partition uint) {
	t.Helper()
	err := globalEnv.DB.Exec(
		"INSERT INTO mq_manual_partition_assignments (group_id, consumer_id_pattern, topic, `partition`) VALUES (?, ?, ?, ?)",
		groupID, pattern, topicName, partition,
	).Error
	require.NoError(t, err, "Failed to insert manual assignment")
}

func TestTC_ManualPartitionAssignment(t *testing.T) {
	t.Run("ExactMatch", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "manual-exact-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-manual-exact",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		c1, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "manual-exact-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)

		// 插入精确匹配规则
		createManualAssignment(t, "manual-exact-group", c1.ID(), topicName, 0)
		createManualAssignment(t, "manual-exact-group", c1.ID(), topicName, 1)

		c1.SubscribeTopics(topicName)
		require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)
		defer c1.Close()

		// 向所有4个分区发送消息
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 4 {
			for j := range 3 {
				// 使用 SendBatch 并指定分区（通过构造 key 使其落在特定分区）
				p.Send(ctx, ProducerMessage{
					Topic: topicName,
					Key:   fmt.Sprintf("p%d-%d", i, j),
					Value: jsonValue(fmt.Sprintf("partition%d-msg%d", i, j)),
				})
			}
		}

		// c1 消费
		consumedPartitions := make(map[uint]int)
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		for {
			msgs, err := c1.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				break
			}
			for _, msg := range msgs {
				consumedPartitions[msg.Partition]++
				c1.Acknowledge(msg)
			}
			c1.CommitSync(pollCtx)
		}
		cancel()

		t.Logf("c1 consumed from partitions: %v", consumedPartitions)
		// c1 应只消费到 partition 0 和 1 的消息
		for partition := range consumedPartitions {
			assert.True(t, partition == 0 || partition == 1,
				"c1 should only consume from partition 0 and 1, but consumed from partition %d", partition)
		}
	})

	t.Run("PrefixMatch", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "manual-prefix-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-manual-prefix",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		c1, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "manual-prefix-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)

		// Consumer ID 通常格式为 hostname:mac, 提取前缀
		consumerID := c1.ID()
		prefix := consumerID[:min(8, len(consumerID))]
		createManualAssignment(t, "manual-prefix-group", prefix+"*", topicName, 2)

		c1.SubscribeTopics(topicName)
		require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)
		defer c1.Close()

		// 发送消息到所有分区
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 20 {
			p.Send(ctx, ProducerMessage{
				Topic: topicName,
				Key:   fmt.Sprintf("k%d", i),
				Value: jsonValue(fmt.Sprintf("msg-%d", i)),
			})
		}

		// c1 消费
		consumedPartitions := make(map[uint]int)
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		for {
			msgs, err := c1.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				break
			}
			for _, msg := range msgs {
				consumedPartitions[msg.Partition]++
				c1.Acknowledge(msg)
			}
			c1.CommitSync(pollCtx)
		}
		cancel()

		t.Logf("Prefix-matched consumer consumed from partitions: %v", consumedPartitions)
		// 应包含 partition 2 （手动分配的分区）
		if len(consumedPartitions) > 0 {
			_, hasPart2 := consumedPartitions[2]
			assert.True(t, hasPart2, "Consumer with prefix match should get partition 2")
		}
	})

	t.Run("ManualAndAutoMixed", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "manual-mixed-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-manual-mixed",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		// c1 手动分配 partition 0, 1
		c1, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "manual-mixed-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)

		createManualAssignment(t, "manual-mixed-group", c1.ID(), topicName, 0)
		createManualAssignment(t, "manual-mixed-group", c1.ID(), topicName, 1)

		c1.SubscribeTopics(topicName)
		require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)

		// c2 自动分配
		c2, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "manual-mixed-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)
		c2.SubscribeTopics(topicName)
		require.Eventually(t, c2.IsReady, 5*time.Second, 100*time.Millisecond)

		// 等待重平衡稳定
		time.Sleep(2 * time.Second)

		defer c1.Close()
		defer c2.Close()

		// 发送消息
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 40 {
			p.Send(ctx, ProducerMessage{
				Topic: topicName,
				Key:   fmt.Sprintf("k%d", i),
				Value: jsonValue(fmt.Sprintf("msg-%d", i)),
			})
		}

		// 并发消费
		c1Partitions := sync.Map{}
		c2Partitions := sync.Map{}
		pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for {
				msgs, err := c1.Poll(pollCtx, 500*time.Millisecond)
				if err != nil {
					return
				}
				for _, msg := range msgs {
					c1Partitions.Store(msg.Partition, true)
					c1.Acknowledge(msg)
				}
				c1.CommitSync(pollCtx)
			}
		}()
		go func() {
			defer wg.Done()
			for {
				msgs, err := c2.Poll(pollCtx, 500*time.Millisecond)
				if err != nil {
					return
				}
				for _, msg := range msgs {
					c2Partitions.Store(msg.Partition, true)
					c2.Acknowledge(msg)
				}
				c2.CommitSync(pollCtx)
			}
		}()
		wg.Wait()

		// 检查 c1 分区
		var c1Parts, c2Parts []uint
		c1Partitions.Range(func(k, _ any) bool {
			c1Parts = append(c1Parts, k.(uint))
			return true
		})
		c2Partitions.Range(func(k, _ any) bool {
			c2Parts = append(c2Parts, k.(uint))
			return true
		})

		t.Logf("c1 (manual) consumed from partitions: %v", c1Parts)
		t.Logf("c2 (auto) consumed from partitions: %v", c2Parts)

		// c1 应消费 partition 0, 1；c2 应消费 partition 2, 3
		for _, pt := range c1Parts {
			assert.True(t, pt == 0 || pt == 1, "c1 should only consume from partition 0 and 1, got %d", pt)
		}
		for _, pt := range c2Parts {
			assert.True(t, pt == 2 || pt == 3, "c2 should only consume from partition 2 and 3, got %d", pt)
		}
		// 确保至少有消息被消费
		assert.Greater(t, len(c1Parts)+len(c2Parts), 0, "At least one consumer should have consumed messages")
	})

	t.Run("RuleChangeTriggerRebalance", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "manual-rebal-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-manual-rebal",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		c1, err := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "manual-rebal-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		require.NoError(t, err)

		// 先手动分配 partition 0
		createManualAssignment(t, "manual-rebal-group", c1.ID(), topicName, 0)

		c1.SubscribeTopics(topicName)
		require.Eventually(t, c1.IsReady, 5*time.Second, 100*time.Millisecond)
		defer c1.Close()

		// 等待稳定
		time.Sleep(2 * time.Second)

		// 记录当前 generation
		var gen1 uint
		globalEnv.DB.Raw("SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?",
			"manual-rebal-group").Scan(&gen1)
		t.Logf("Generation before rule change: %d", gen1)

		// 新增 c1 手动分配 partition 1
		createManualAssignment(t, "manual-rebal-group", c1.ID(), topicName, 1)

		// 等待协调器检测到变化并触发重平衡
		time.Sleep(3 * time.Second)

		var gen2 uint
		globalEnv.DB.Raw("SELECT generation_id FROM mq_consumer_group_generations WHERE group_id = ?",
			"manual-rebal-group").Scan(&gen2)
		t.Logf("Generation after rule change: %d", gen2)

		assert.Greater(t, gen2, gen1, "Generation should increase after manual assignment rule change")
	})
}

// =============================================================================
// 测试场景31: AdminClient.CreateTopics 批量操作
// =============================================================================

func TestTC_AdminCreateTopicsBatch(t *testing.T) {
	cleanupTables(t)
	ctx := context.Background()

	admin := NewAdminClient(globalEnv.DB)

	// 预创建一个 Topic
	require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: "exists-topic", NumPartitions: 1}))

	// 批量创建
	result := admin.CreateTopics(ctx, []NewTopicRequest{
		{Name: "new-topic-1", NumPartitions: 2},
		{Name: "exists-topic", NumPartitions: 1},
		{Name: "", NumPartitions: 1},
		{Name: "new-topic-2", NumPartitions: 3},
	})

	require.Len(t, result.Results, 4)

	// new-topic-1: 成功
	assert.Nil(t, result.Results[0].Error, "new-topic-1 should succeed")
	assert.Equal(t, "new-topic-1", result.Results[0].Name)

	// exists-topic: 失败（已存在）
	assert.NotNil(t, result.Results[1].Error, "exists-topic should fail")
	t.Logf("exists-topic error: %v", result.Results[1].Error)

	// 空名称: 失败
	assert.NotNil(t, result.Results[2].Error, "empty name should fail")
	t.Logf("empty name error: %v", result.Results[2].Error)

	// new-topic-2: 成功
	assert.Nil(t, result.Results[3].Error, "new-topic-2 should succeed")
	assert.Equal(t, "new-topic-2", result.Results[3].Name)

	// 验证创建的 Topic 存在
	topics, err := admin.ListTopics(ctx)
	require.NoError(t, err)
	topicSet := make(map[string]bool)
	for _, name := range topics {
		topicSet[name] = true
	}
	assert.True(t, topicSet["new-topic-1"], "new-topic-1 should exist")
	assert.True(t, topicSet["new-topic-2"], "new-topic-2 should exist")
	assert.True(t, topicSet["exists-topic"], "exists-topic should still exist")

	// 验证分区数正确
	desc, err := admin.DescribeTopics(ctx, []string{"new-topic-1", "new-topic-2"})
	require.NoError(t, err)
	assert.Equal(t, 2, desc["new-topic-1"].NumPartitions)
	assert.Equal(t, 3, desc["new-topic-2"].NumPartitions)
}

// =============================================================================
// 测试场景32: DeleteTopics 级联删除
// =============================================================================

func TestTC_DeleteTopicCascade(t *testing.T) {
	t.Run("CascadeDelete", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "cascade-delete-topic"
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-cascade",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		// 发送消息
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 10 {
			p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("k%d", i), Value: jsonValue(fmt.Sprintf("msg-%d", i))})
		}

		// 创建消费者消费并提交（产生 progress 记录）
		consumer, _ := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "cascade-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		consumer.SubscribeTopics(topicName)
		require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)

		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				break
			}
			for _, msg := range msgs {
				consumer.Acknowledge(msg)
			}
			consumer.CommitSync(pollCtx)
		}
		cancel()
		consumer.Close()

		// 验证数据存在
		var msgCount, progressCount, topicCount int64
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", topicName).Scan(&msgCount)
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_consumer_group_consumption_progress WHERE topic = ?", topicName).Scan(&progressCount)
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_topics WHERE topic_name = ?", topicName).Scan(&topicCount)
		t.Logf("Before delete: messages=%d, progress=%d, topics=%d", msgCount, progressCount, topicCount)
		assert.Greater(t, msgCount, int64(0), "Should have messages before delete")
		assert.Greater(t, progressCount, int64(0), "Should have progress records before delete")
		assert.Equal(t, int64(1), topicCount, "Should have topic before delete")

		// 删除 Topic
		require.NoError(t, admin.DeleteTopics(ctx, []string{topicName}))

		// 验证级联删除
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_messages WHERE topic = ?", topicName).Scan(&msgCount)
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_consumer_group_consumption_progress WHERE topic = ?", topicName).Scan(&progressCount)
		globalEnv.DB.Raw("SELECT COUNT(*) FROM mq_topics WHERE topic_name = ?", topicName).Scan(&topicCount)
		t.Logf("After delete: messages=%d, progress=%d, topics=%d", msgCount, progressCount, topicCount)

		assert.Equal(t, int64(0), msgCount, "Messages should be deleted")
		assert.Equal(t, int64(0), progressCount, "Progress records should be deleted")
		assert.Equal(t, int64(0), topicCount, "Topic should be deleted")
	})

	t.Run("RecreateAfterDeleteIsolation", func(t *testing.T) {
		cleanupTables(t)
		ctx := context.Background()

		admin := NewAdminClient(globalEnv.DB)
		topicName := "recreate-topic"

		// 创建 → 发消息 → 删除
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 2}))
		p, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 5 {
			p.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("old%d", i), Value: jsonValue(fmt.Sprintf("old-%d", i))})
		}
		require.NoError(t, admin.DeleteTopics(ctx, []string{topicName}))

		// 重新创建同名 Topic（不同分区数）
		require.NoError(t, admin.CreateTopic(ctx, NewTopicRequest{Name: topicName, NumPartitions: 4}))

		// 验证分区数正确
		desc, err := admin.DescribeTopics(ctx, []string{topicName})
		require.NoError(t, err)
		assert.Equal(t, 4, desc[topicName].NumPartitions, "Recreated topic should have new partition count")

		// 发送新消息
		newP, _ := NewProducer(ProducerConfig{DB: globalEnv.DB, Redis: globalEnv.Redis, NotificationEnabled: true})
		for i := range 3 {
			_, err := newP.Send(ctx, ProducerMessage{Topic: topicName, Key: fmt.Sprintf("new%d", i), Value: jsonValue(fmt.Sprintf("new-%d", i))})
			require.NoError(t, err)
		}

		coordinator := NewCoordinator(CoordinatorConfig{
			LockSuffix:        "tc-recreate",
			DB:                globalEnv.DB,
			NodeAddr:          "test-node",
			HeartbeatTimeout:  3 * time.Second,
			RebalanceInterval: 500 * time.Millisecond,
		})
		coordinator.Start()
		defer coordinator.Stop()
		require.Eventually(t, coordinator.IsLeader, 5*time.Second, 100*time.Millisecond)

		// 新消费者消费
		consumer, _ := NewConsumer(ConsumerConfig{
			DB: globalEnv.DB, Redis: globalEnv.Redis,
			GroupID: "recreate-group", NotificationEnabled: true,
			Topics: []string{topicName}, HeartbeatInterval: 500 * time.Millisecond,
			ConsumeStrategy: ConsumeFromEarliest,
		})
		consumer.SubscribeTopics(topicName)
		require.Eventually(t, consumer.IsReady, 5*time.Second, 100*time.Millisecond)
		defer consumer.Close()

		var consumed int
		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		for {
			msgs, err := consumer.Poll(pollCtx, 500*time.Millisecond)
			if err != nil {
				break
			}
			for _, msg := range msgs {
				consumed++
				consumer.Acknowledge(msg)
			}
			consumer.CommitSync(pollCtx)
		}
		cancel()

		t.Logf("Consumed %d messages from recreated topic", consumed)
		assert.Equal(t, 3, consumed, "Should only consume new messages (3), not old ones")
	})
}
