package examples

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// StrategyDemo 演示不同的消费策略
func StrategyDemo() {
	fmt.Println("🎯 DBMQ 消费策略演示")
	fmt.Println("===================")
	fmt.Println("📚 本演示将展示三种消费策略的工作方式：")
	fmt.Println("   1. ConsumeFromEarliest - 从最早的消息开始消费")
	fmt.Println("   2. ConsumeFromLatest - 从最新的消息开始消费")
	fmt.Println("   3. ConsumeFromCommitted - 从已提交的偏移量开始消费（默认）")
	fmt.Println()

	// 1. 初始化数据库连接
	fmt.Println("🔌 初始化数据库连接...")
	dbClient, err := db.InitMySQL(db.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
		DBName:   "dbmq",
	})
	if err != nil {
		log.Fatalf("❌ 数据库连接失败: %v", err)
	}
	fmt.Println("✅ 数据库连接成功")

	// 2. 初始化Redis连接
	fmt.Println("🔌 初始化Redis连接...")
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       2,
	})

	// 测试Redis连接
	_, err = redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Printf("⚠️  Redis连接失败，将禁用通知功能: %v", err)
		redisClient = nil
	} else {
		fmt.Println("✅ Redis连接成功")
	}

	// 3. 创建管理客户端
	fmt.Println("⚙️  创建管理客户端...")
	admin, err := dbmq.NewAdminClient(dbmq.AdminConfig{DB: dbClient})
	if err != nil {
		log.Fatalf("❌ 创建管理客户端失败: %v", err)
	}
	fmt.Println("✅ 管理客户端创建成功")

	// 4. 创建主题
	topicName := "策略演示主题"
	fmt.Printf("📋 创建主题: %s\n", topicName)
	err = admin.CreateTopic(context.Background(), dbmq.NewTopicRequest{
		Name:          topicName,
		NumPartitions: 1,
	})
	if err != nil {
		var topicExistsErr *dbmq.ErrTopicAlreadyExists
		if !errors.As(err, &topicExistsErr) {
			log.Fatalf("❌ 创建主题失败: %v", err)
		}
		fmt.Printf("ℹ️  主题 %s 已存在，继续使用\n", topicName)
	} else {
		fmt.Println("✅ 主题创建成功")
	}

	// 5. 创建生产者并发送一些历史消息
	fmt.Println("📤 创建生产者并发送历史消息...")
	producer, err := dbmq.NewProducer(dbmq.ProducerConfig{
		DB:    dbClient,
		Redis: redisClient,
	})
	if err != nil {
		log.Fatalf("❌ 创建生产者失败: %v", err)
	}
	defer producer.Close()

	// 发送10条历史消息
	for i := 1; i <= 10; i++ {
		message := &dbmq.ProducerMessage{
			Topic: topicName,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("历史消息 #%d - 时间: %s", i, time.Now().Format("15:04:05"))),
		}
		result, err := producer.Send(context.Background(), message)
		if err != nil {
			log.Printf("❌ 发送历史消息失败: %v", err)
			continue
		}
		fmt.Printf("📤 发送历史消息 #%d (偏移量: %d)\n", i, result.Offset)
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Println("✅ 历史消息发送完成")
	fmt.Println()

	// 6. 演示不同的消费策略
	demonstrateStrategy(dbClient, redisClient, topicName, "Earliest策略", dbmq.ConsumeFromEarliest)
	demonstrateStrategy(dbClient, redisClient, topicName, "Latest策略", dbmq.ConsumeFromLatest)
	demonstrateStrategy(dbClient, redisClient, topicName, "Committed策略", dbmq.ConsumeFromCommitted)

	fmt.Println()
	fmt.Println("🎯 消费策略演示完成！")
	fmt.Println("📖 总结：")
	fmt.Println("   - Earliest策略：消费组第一次消费时从分区开头开始")
	fmt.Println("   - Latest策略：消费组第一次消费时跳过历史消息，从最新开始")
	fmt.Println("   - Committed策略：优先使用已提交偏移量，没有时从最新开始")
	fmt.Println("   - 所有策略都会在有已提交偏移量时从上次位置继续消费")
}

// demonstrateStrategy 演示特定的消费策略
func demonstrateStrategy(db *gorm.DB, redis *redis.Client, topicName, strategyName string, strategy dbmq.ConsumeStrategy) {
	fmt.Printf("🔍 演示 %s\n", strategyName)
	fmt.Println("----------------------------------------")

	// 创建唯一的消费组名
	groupID := fmt.Sprintf("策略演示组-%s", strategy.String())

	// 清除该消费组的历史偏移量，确保演示策略的第一次行为
	clearConsumerGroupOffsets(db, groupID)

	// 创建消费者
	consumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
		DB:                  db,
		Redis:               redis,
		GroupID:             groupID,
		NotificationEnabled: false, // 为了演示清晰，暂时禁用通知
		HeartbeatInterval:   3 * time.Second,
		Topics:              []string{topicName},
		ConsumeStrategy:     strategy, // 设置消费策略
		PollFetchLimit:      5,
		PollFetchTimeout:    3 * time.Second,
	})
	if err != nil {
		log.Printf("❌ 创建消费者失败: %v", err)
		return
	}
	defer consumer.Close()

	// 订阅主题
	err = consumer.SubscribeTopics(topicName)
	if err != nil {
		log.Printf("❌ 订阅主题失败: %v", err)
		return
	}

	// 等待消费者准备就绪
	fmt.Printf("⏳ 等待消费者准备就绪...\n")
	for !consumer.IsReady() {
		time.Sleep(500 * time.Millisecond)
	}
	fmt.Printf("✅ 消费者已就绪\n")

	// 拉取消息
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Printf("📥 开始拉取消息（策略: %s）...\n", strategy.String())
	messageCount := 0

	for messageCount < 3 { // 最多拉取3条消息用于演示
		messages, err := consumer.Poll(ctx, 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				break // 超时退出
			}
			log.Printf("❌ 拉取消息失败: %v", err)
			continue
		}

		if len(messages) == 0 {
			fmt.Printf("📭 没有新消息\n")
			break
		}

		for _, msg := range messages {
			messageCount++
			fmt.Printf("📨 收到消息 #%d - 偏移量: %d, 内容: %s\n",
				messageCount, msg.Offset, string(msg.Value))

			if messageCount >= 3 {
				break
			}
		}

		// 提交偏移量
		if err := consumer.CommitSync(); err != nil {
			log.Printf("❌ 提交偏移量失败: %v", err)
		}
	}

	fmt.Printf("✅ %s 演示完成，共消费 %d 条消息\n", strategyName, messageCount)
	fmt.Println()
}
