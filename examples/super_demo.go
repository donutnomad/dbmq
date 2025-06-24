// Package examples 展示DBMQ的完整使用场景
// 创建订单主题的超级演示：两个消费组同时订阅，生产者发送消息
package examples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/db"
	"gorm.io/gorm"
	"log"
	"sync"
	"time"
)

// OrderMessage 订单消息结构
type OrderMessage struct {
	OrderID    string    `json:"order_id"`    // 订单ID
	CustomerID string    `json:"customer_id"` // 客户ID
	Amount     float64   `json:"amount"`      // 订单金额
	Status     string    `json:"status"`      // 订单状态
	CreatedAt  time.Time `json:"created_at"`  // 创建时间
}

// SuperDemo 超级演示函数
// 展示完整的DBMQ使用场景：创建主题、多消费组订阅、生产消息、消费消息
func SuperDemo() {
	fmt.Println("🚀 开始DBMQ超级演示...")
	fmt.Println("📋 场景：创建订单主题，三个消费组同时订阅处理订单消息")
	fmt.Println("💡 演示特色：同时展示手动提交和自动提交两种偏移量提交模式")
	fmt.Println("   - 消费组001: 手动提交模式（消息处理后立即提交偏移量）")
	fmt.Println("   - 消费组002: 自动提交模式（每3秒自动提交一次偏移量）")
	fmt.Println("   - 消费组003: 自动提交模式（每4秒自动提交一次偏移量）")
	fmt.Println("⏰ 演示时长：20秒")
	fmt.Println()

	// 1. 初始化数据库连接
	fmt.Println("🔧 初始化数据库连接...")
	mysqlConfig := db.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
		DBName:   "dbmq_demo",
	}

	// 创建数据库（如果不存在）
	err := db.CreateDatabaseIfNotExists(mysqlConfig)
	if err != nil {
		log.Fatalf("❌ 创建数据库失败: %v", err)
	}

	// 初始化数据库连接
	dbClient, err := db.InitMySQL(mysqlConfig)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}

	// 应用数据库模式
	err = db.ApplySchemas(dbClient)
	if err != nil {
		log.Fatalf("❌ 应用数据库模式失败: %v", err)
	}

	// 初始化Redis连接（可选，用于实时通知优化）
	redisConfig := db.RedisConfig{
		Host: "localhost",
		Port: 6379,
		DB:   2,
	}
	redisClient, err := db.InitRedis(redisConfig)
	if err != nil {
		fmt.Printf("⚠️  Redis连接失败（将使用轮询模式）: %v\n", err)
		redisClient = nil
	}

	fmt.Println("✅ 数据库和Redis连接初始化完成")
	fmt.Println()

	// 2. 启动协调器（必须在消费者之前启动）
	fmt.Println("⚖️  启动协调器...")
	coordinator := dbmq.NewCoordinator(dbmq.CoordinatorConfig{
		DB:                     dbClient,
		HeartbeatTimeout:       30 * time.Second,   // 心跳超时时间，增加到30秒
		RebalanceInterval:      3 * time.Second,    // 重新均衡检查间隔，设置为3秒确保快速重新均衡
		RebalanceTimeout:       60 * time.Second,   // 重新均衡操作超时，增加到60秒
		RetentionCheckInterval: 1 * time.Hour,      // 消息保留检查间隔
		DefaultRetentionAge:    7 * 24 * time.Hour, // 默认7天保留期
	})
	coordinator.Start()
	defer coordinator.Stop()

	// 等待协调器成为领导者
	fmt.Println("⏳ 等待协调器成为领导者...")
	for !coordinator.IsLeader() {
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("✅ 协调器已成为领导者")
	fmt.Println()

	// 3. 创建AdminClient并创建主题
	fmt.Println("📝 创建管理客户端...")
	admin, err := dbmq.NewAdminClient(dbmq.AdminConfig{
		DB: dbClient,
	})
	if err != nil {
		log.Fatalf("❌ 创建管理客户端失败: %v", err)
	}
	defer admin.Close()

	// 创建"创建订单"主题
	fmt.Println("🎯 创建主题: 创建订单")
	topicName := "创建订单"
	topicReq := dbmq.NewTopicRequest{
		Name:          topicName,
		NumPartitions: 1, // 1个分区
	}

	// 尝试创建主题，如果已存在则忽略
	err = createTopicIfNotExists(admin, topicReq)
	if err != nil {
		log.Fatalf("❌ 创建主题失败: %v", err)
	}
	fmt.Printf("✅ 主题创建成功: %s (分区数: %d)\n", topicName, topicReq.NumPartitions)
	fmt.Println()

	// 4. 创建生产者
	fmt.Println("📤 创建生产者...")
	producer, err := dbmq.NewProducer(dbmq.ProducerConfig{
		DB:                   dbClient,
		Redis:                redisClient,
		NotificationEnabled:  redisClient != nil, // 如果Redis可用则启用通知
		NotificationStateTTL: 10 * time.Second,   // 设置通知状态TTL为10秒
	})
	if err != nil {
		log.Fatalf("❌ 创建生产者失败: %v", err)
	}
	defer producer.Close()
	fmt.Println("✅ 生产者创建成功")

	// 5. 创建两个消费组的消费者
	fmt.Println("📥 创建消费者...")

	// 消费组001 - 订单处理服务（手动提交模式）
	consumer001, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "消费组001",
		NotificationEnabled: redisClient != nil, // 重新启用Redis通知
		HeartbeatInterval:   5 * time.Second,    // 增加心跳间隔到5秒
		Topics:              []string{topicName},
		PollFetchLimit:      10,
		PollFetchTimeout:    5 * time.Second, // 增加拉取超时到5秒
		EnableAutoCommit:    false,           // 禁用自动提交，使用手动提交
	})
	if err != nil {
		log.Fatalf("❌ 创建消费者001失败: %v", err)
	}
	defer consumer001.Close()

	// 消费组002 - 数据分析服务（自动提交模式）
	consumer002, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "消费组002",
		NotificationEnabled: redisClient != nil, // 重新启用Redis通知
		HeartbeatInterval:   5 * time.Second,    // 增加心跳间隔到5秒
		Topics:              []string{topicName},
		PollFetchLimit:      10,
		PollFetchTimeout:    5 * time.Second, // 增加拉取超时到5秒
		EnableAutoCommit:    true,            // 启用自动提交
		AutoCommitInterval:  3 * time.Second, // 每3秒自动提交一次
	})
	if err != nil {
		log.Fatalf("❌ 创建消费者002失败: %v", err)
	}
	defer consumer002.Close()

	// 消费组003 - 从最新消息开始消费（自动提交模式）
	consumer003, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             "消费组003",
		NotificationEnabled: redisClient != nil, // 重新启用Redis通知
		HeartbeatInterval:   5 * time.Second,    // 增加心跳间隔到5秒
		Topics:              []string{topicName},
		PollFetchLimit:      10,
		PollFetchTimeout:    5 * time.Second, // 增加拉取超时到5秒
		ConsumeStrategy:     dbmq.ConsumeFromLatest,
		EnableAutoCommit:    true,            // 启用自动提交
		AutoCommitInterval:  4 * time.Second, // 每4秒自动提交一次
	})
	if err != nil {
		log.Fatalf("❌ 创建消费者003失败: %v", err)
	}
	defer consumer003.Close()

	fmt.Println("✅ 消费者创建成功")
	fmt.Println("   - 消费组001: 订单处理服务（手动提交模式）")
	fmt.Println("   - 消费组002: 数据分析服务（自动提交模式，间隔: 3秒）")
	fmt.Println("   - 消费组003: 从最新消息开始消费（自动提交模式，间隔: 4秒）")
	fmt.Println()

	// 6. 启动消费者订阅
	fmt.Println("🔄 启动消费者订阅...")
	err = consumer001.SubscribeTopics(topicName)
	if err != nil {
		log.Fatalf("❌ 消费者001订阅失败: %v", err)
	}

	err = consumer002.SubscribeTopics(topicName)
	if err != nil {
		log.Fatalf("❌ 消费者002订阅失败: %v", err)
	}

	err = consumer003.SubscribeTopics(topicName)
	if err != nil {
		log.Fatalf("❌ 消费者003订阅失败: %v", err)
	}

	fmt.Println("✅ 消费者订阅启动成功")

	// // 等待消费者完成分区分配
	// fmt.Println("⏳ 等待消费者完成分区分配...")
	// time.Sleep(3 * time.Second) // 给协调器时间进行重新均衡
	// fmt.Println("✅ 消费者分区分配完成")
	// fmt.Println()

	// 7. 创建上下文和等待组，用于协调所有goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	//// 清除演示用消费组的历史偏移量，确保从最新消息开始消费
	//fmt.Println("🧹 清除演示用消费组的历史偏移量...")
	//clearConsumerGroupOffsets(dbClient, "消费组001")
	//clearConsumerGroupOffsets(dbClient, "消费组002")
	//fmt.Println("✅ 历史偏移量清除完成，消费者将从最新消息开始消费")
	//fmt.Println()

	var wg sync.WaitGroup

	// 8. 启动消费者001的消费循环（手动提交模式）
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessagesWithManualCommit(ctx, consumer001, "消费者001", "订单处理服务")
	}()

	// 9. 启动消费者002的消费循环（自动提交模式）
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessagesWithAutoCommit(ctx, consumer002, "消费者002", "数据分析服务")
	}()

	// 10. 启动消费者003的消费循环（自动提交模式）
	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeMessagesWithAutoCommit(ctx, consumer003, "消费者003", "从最新的地方开始消费")
	}()

	// 11. 启动生产者发送消息
	wg.Add(1)
	go func() {
		defer wg.Done()
		produceOrderMessages(ctx, producer, topicName)
	}()

	fmt.Println("🎬 演示开始！所有服务已启动...")
	fmt.Println("📊 实时监控消息流转...")
	fmt.Println()

	// 12. 等待所有goroutine完成或超时
	wg.Wait()

	fmt.Println()
	fmt.Println("🏁 演示结束！")
	fmt.Println("📈 所有消费者和生产者已优雅关闭")
	fmt.Println("💾 消息已持久化到数据库")
}

// createTopicIfNotExists 创建主题，如果已存在则忽略
func createTopicIfNotExists(admin *dbmq.AdminClient, req dbmq.NewTopicRequest) error {
	err := admin.CreateTopic(context.Background(), req)
	if err != nil {
		var topicExistsErr *dbmq.ErrTopicAlreadyExists
		if errors.As(err, &topicExistsErr) {
			fmt.Printf("ℹ️  主题 %s 已存在，跳过创建\n", req.Name)
			return nil
		}
		return err
	}
	return nil
}

// clearConsumerGroupOffsets 清除指定消费组的所有偏移量提交记录
func clearConsumerGroupOffsets(db *gorm.DB, groupID string) {
	result := db.Exec("DELETE FROM mq_consumer_group_offsets WHERE group_id = ?", groupID)
	if result.Error != nil {
		log.Printf("⚠️  清除消费组 %s 偏移量时出错: %v", groupID, result.Error)
	} else {
		log.Printf("🗑️  已清除消费组 %s 的 %d 条偏移量记录", groupID, result.RowsAffected)
	}
}

// produceOrderMessages 生产者发送订单消息
func produceOrderMessages(ctx context.Context, producer *dbmq.Producer, topicName string) {
	fmt.Println("📤 生产者开始发送订单消息...")

	orderCounter := time.Now().Unix()
	ticker := time.NewTicker(2 * time.Second) // 每2秒发送一条消息
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("📤 生产者收到停止信号，正在关闭...")
			return
		case <-ticker.C:
			// 创建订单消息
			order := OrderMessage{
				OrderID:    fmt.Sprintf("ORDER-%06d", orderCounter),
				CustomerID: fmt.Sprintf("CUST-%04d", orderCounter%100+1),
				Amount:     float64(orderCounter*10) + 99.99,
				Status:     "CREATED",
				CreatedAt:  time.Now(),
			}

			// 序列化为JSON
			orderJSON, err := json.Marshal(order)
			if err != nil {
				log.Printf("❌ 序列化订单消息失败: %v", err)
				continue
			}

			// 发送消息
			message := &dbmq.ProducerMessage{
				Topic: topicName,
				Key:   []byte(order.OrderID), // 使用订单ID作为消息键
				Value: orderJSON,
				Headers: map[string]string{
					"Content-Type": "application/json",
					"Source":       "order-service",
					"Version":      "1.0",
				},
			}

			result, err := producer.Send(ctx, message)
			if err != nil {
				log.Printf("❌ 发送消息失败: %v", err)
				continue
			}

			fmt.Printf("📤 [生产者] 发送订单消息: %s (分区: %d, 偏移量: %d, 金额: %.2f)\n",
				order.OrderID, result.Partition, result.Offset, order.Amount)

			orderCounter++
		}
	}
}

// consumeMessagesWithManualCommit 手动提交模式的消费者消费消息循环
func consumeMessagesWithManualCommit(ctx context.Context, consumer *dbmq.Consumer, consumerName, serviceName string) {
	fmt.Printf("📥 [%s] %s 开始消费消息...(手动提交模式)\n", consumerName, serviceName)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("📥 [%s] 收到停止信号，正在关闭...\n", consumerName)
			return
		default:
			// 拉取消息
			messages, err := consumer.Poll(ctx, 1*time.Second)
			if err != nil {
				// 检查是否是重新均衡错误
				var rebalanceErr *dbmq.ErrRebalanceInProgress
				if errors.As(err, &rebalanceErr) {
					fmt.Printf("⚖️  [%s] 正在进行重新均衡，等待完成...\n", consumerName)
					time.Sleep(500 * time.Millisecond)
					continue
				}

				// 检查是否是上下文取消
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				log.Printf("❌ [%s] 拉取消息失败: %v\n", consumerName, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// 逐个处理消息，并精确提交每个消息的偏移量
			for i, msg := range messages {
				// 处理消息
				success := processOrderMessageWithResult(msg, consumerName, serviceName)

				if success {
					// 处理成功，提交这个具体消息的偏移量
					if err := consumer.CommitMessage(msg); err != nil {
						log.Printf("❌ [%s] 提交消息偏移量失败 (消息ID: %d): %v\n", consumerName, msg.Offset, err)
					} else {
						fmt.Printf("✅ [%s] 精确提交消息偏移量: %d (第%d/%d条)\n",
							consumerName, msg.Offset, i+1, len(messages))
					}
				} else {
					// 处理失败，不提交偏移量，这条消息会在下次重新消费
					fmt.Printf("❌ [%s] 消息处理失败，不提交偏移量: %d\n", consumerName, msg.Offset)
				}
			}
		}
	}
}

// consumeMessagesWithAutoCommit 自动提交模式的消费者消费消息循环
func consumeMessagesWithAutoCommit(ctx context.Context, consumer *dbmq.Consumer, consumerName, serviceName string) {
	fmt.Printf("📥 [%s] %s 开始消费消息...(自动提交模式)\n", consumerName, serviceName)
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("📥 [%s] 收到停止信号，正在关闭...\n", consumerName)
			return
		default:
			// 拉取消息
			messages, err := consumer.Poll(ctx, 1*time.Second)
			if err != nil {
				// 检查是否是重新均衡错误
				var rebalanceErr *dbmq.ErrRebalanceInProgress
				if errors.As(err, &rebalanceErr) {
					fmt.Printf("⚖️  [%s] 正在进行重新均衡，等待完成...\n", consumerName)
					time.Sleep(500 * time.Millisecond)
					continue
				}

				// 检查是否是上下文取消
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}

				log.Printf("❌ [%s] 拉取消息失败: %v\n", consumerName, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// 处理接收到的消息
			for _, msg := range messages {
				processOrderMessage(msg, consumerName, serviceName)
			}

			// 自动提交模式下不需要手动提交偏移量
			// 偏移量会由自动提交循环定期提交
			if len(messages) > 0 {
				fmt.Printf("📦 [%s] 处理了 %d 条消息，等待自动提交偏移量\n", consumerName, len(messages))
			}
		}
	}
}

// processOrderMessage 处理订单消息（无返回值版本，用于自动提交模式）
func processOrderMessage(msg dbmq.ConsumerMessage, consumerName, serviceName string) {
	processOrderMessageWithResult(msg, consumerName, serviceName)
}

// processOrderMessageWithResult 处理订单消息并返回处理结果（用于手动提交模式）
func processOrderMessageWithResult(msg dbmq.ConsumerMessage, consumerName, serviceName string) bool {
	// 解析订单消息
	var order OrderMessage
	err := json.Unmarshal(msg.Value, &order)
	if err != nil {
		log.Printf("❌ [%s] 解析订单消息失败: %v\n", consumerName, err)
		return false // 解析失败，返回false
	}

	// 模拟不同服务的处理逻辑
	success := true // 默认处理成功

	switch serviceName {
	case "订单处理服务":
		fmt.Printf("🔄 [%s] 处理订单: %s | 客户: %s | 金额: %.2f | 状态: %s (手动提交)\n",
			consumerName, order.OrderID, order.CustomerID, order.Amount, order.Status)

		// 模拟订单处理时间
		time.Sleep(100 * time.Millisecond)

		// 模拟处理失败的情况（比如金额异常）
		if order.Amount < 0 {
			fmt.Printf("❌ [%s] 订单处理失败: %s (金额异常: %.2f)\n", consumerName, order.OrderID, order.Amount)
			success = false
		} else {
			fmt.Printf("✅ [%s] 订单处理完成: %s\n", consumerName, order.OrderID)
		}

	case "数据分析服务":
		fmt.Printf("📊 [%s] 分析订单数据: %s | 金额: %.2f | 时间: %s (自动提交)\n",
			consumerName, order.OrderID, order.Amount, order.CreatedAt.Format("15:04:05"))

		// 模拟数据分析时间
		time.Sleep(50 * time.Millisecond)

		fmt.Printf("📈 [%s] 数据分析完成: %s (客户群体分析已更新)\n", consumerName, order.OrderID)

	case "从最新的地方开始消费":
		fmt.Printf("🆕 [%s] 处理最新订单: %s | 金额: %.2f | 时间: %s (自动提交)\n",
			consumerName, order.OrderID, order.Amount, order.CreatedAt.Format("15:04:05"))

		// 模拟处理时间
		time.Sleep(30 * time.Millisecond)

		fmt.Printf("✨ [%s] 最新订单处理完成: %s\n", consumerName, order.OrderID)
	}

	// 显示消息元数据
	fmt.Printf("   📋 [%s] 消息元数据 - OrderID: %s, 主题: %s | 分区: %d | 偏移量: %d | 时间戳: %s\n",
		consumerName, order.OrderID, msg.Topic, msg.Partition, msg.Offset, msg.Timestamp.Format("15:04:05"))

	return success
}
