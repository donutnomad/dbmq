package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	dbmq "github.com/donutnomad/dbmq"
)

func main() {
	fmt.Println("🚀 启动 DBMQ REST API 服务器和监控仪表板")
	fmt.Println("=" + string(make([]byte, 50)))

	// 初始化数据库连接
	db, err := initDatabase()
	if err != nil {
		log.Fatalf("❌ 数据库初始化失败: %v", err)
	}
	defer func() {
		sqlDB, _ := db.DB()
		sqlDB.Close()
	}()

	fmt.Println("✅ 数据库连接成功")

	// 初始化Redis连接（可选）
	redisClient := initRedis()
	if redisClient != nil {
		defer redisClient.Close()
		fmt.Println("✅ Redis连接成功")
	}

	// 演示批量发送和Redis缓存功能
	if redisClient != nil {
		go demonstrateBatchSendAndCache(db, redisClient)
	}

	// 创建REST API服务器
	apiConfig := dbmq.RestAPIConfig{
		DB:     db,
		Port:   8080,
		Host:   "localhost",
		Prefix: "/api/v1",
	}

	server, err := dbmq.NewRestAPIServer(apiConfig)
	if err != nil {
		log.Fatalf("❌ 创建REST API服务器失败: %v", err)
	}

	// 启动服务器
	go func() {
		fmt.Println("🌐 REST API 服务器启动中...")
		fmt.Println("📊 监控仪表板: http://localhost:8080/api/v1/dashboard")
		fmt.Println("🔧 健康检查: http://localhost:8080/api/v1/health")
		fmt.Println("📋 API 接口: http://localhost:8080/api/v1/")
		fmt.Println("")
		fmt.Println("按 Ctrl+C 停止服务器")
		fmt.Println("=" + string(make([]byte, 50)))

		if err := server.Start(); err != nil {
			log.Printf("❌ 服务器启动失败: %v", err)
		}
	}()

	// 等待中断信号
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("\n🛑 接收到停止信号，正在关闭服务器...")

	// 优雅关闭
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Stop(ctx); err != nil {
		log.Printf("❌ 服务器关闭失败: %v", err)
	} else {
		fmt.Println("✅ 服务器已优雅关闭")
	}
}

// 初始化数据库连接
func initDatabase() (*gorm.DB, error) {
	// 简单的数据库连接字符串
	dsn := "root:password@tcp(localhost:3306)/dbmq_demo?charset=utf8mb4&parseTime=True&loc=Local"

	// 连接到数据库
	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %w", err)
	}

	return gormDB, nil
}

// 初始化Redis连接（可选）
func initRedis() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码
		DB:       0,  // 使用默认数据库
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Printf("⚠️ Redis连接失败，将使用数据库模式: %v\n", err)
		return nil
	}

	return rdb
}

// 演示批量发送和Redis缓存功能
func demonstrateBatchSendAndCache(db *gorm.DB, redisClient *redis.Client) {
	fmt.Println("\n🎯 开始演示批量发送和Redis缓存功能...")

	// 等待服务器启动
	time.Sleep(2 * time.Second)

	// 创建生产者配置，启用Redis缓存优化
	producerConfig := dbmq.ProducerConfig{
		DB:                   db,
		Redis:                redisClient,
		NotificationEnabled:  true,
		NotificationStateTTL: 60 * time.Second,
		OffsetCacheEnabled:   true,              // 启用offset缓存
		OffsetCacheTTL:       300 * time.Second, // 5分钟缓存
	}

	producer, err := dbmq.NewProducer(producerConfig)
	if err != nil {
		log.Printf("❌ 创建生产者失败: %v", err)
		return
	}
	defer producer.Close()

	ctx := context.Background()

	// 1. 演示单条消息发送（使用缓存优化）
	fmt.Println("\n📤 演示单条消息发送（Redis缓存优化）...")
	singleMsg := &dbmq.ProducerMessage{
		Topic: "performance_test",
		Key:   []byte("test_key_1"),
		Value: []byte("这是一条使用Redis缓存优化的测试消息"),
		Headers: map[string]string{
			"type":      "single_message",
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}

	result, err := producer.Send(ctx, singleMsg)
	if err != nil {
		log.Printf("❌ 发送单条消息失败: %v", err)
	} else {
		fmt.Printf("✅ 单条消息发送成功 - Topic: %s, Partition: %d, Offset: %d\n",
			result.Topic, result.Partition, result.Offset)
	}

	// 2. 演示批量发送（显著提升性能）
	fmt.Println("\n📦 演示批量发送功能...")

	// 准备批量消息
	var batchMessages []*dbmq.ProducerMessage
	batchSize := 10

	for i := 0; i < batchSize; i++ {
		msg := &dbmq.ProducerMessage{
			Topic: "performance_test",
			Key:   []byte(fmt.Sprintf("batch_key_%d", i)),
			Value: []byte(fmt.Sprintf("批量消息 #%d - 使用Redis缓存优化offset查询", i+1)),
			Headers: map[string]string{
				"type":          "batch_message",
				"batch_id":      "demo_batch_1",
				"message_index": fmt.Sprintf("%d", i),
				"timestamp":     time.Now().Format(time.RFC3339),
			},
		}
		batchMessages = append(batchMessages, msg)
	}

	// 执行批量发送
	startTime := time.Now()
	batchResult, err := producer.SendBatch(ctx, batchMessages)
	duration := time.Since(startTime)

	if err != nil {
		log.Printf("❌ 批量发送失败: %v", err)
	} else {
		fmt.Printf("✅ 批量发送成功！\n")
		fmt.Printf("   📊 发送数量: %d 条消息\n", len(batchResult.Results))
		fmt.Printf("   ⏱️  耗时: %v\n", duration)
		fmt.Printf("   🚀 平均TPS: %.2f 消息/秒\n", float64(len(batchResult.Results))/duration.Seconds())

		// 显示前几条消息的详细信息
		fmt.Println("   📋 消息详情:")
		for i, result := range batchResult.Results {
			if i < 3 { // 只显示前3条
				fmt.Printf("      [%d] Topic: %s, Partition: %d, Offset: %d\n",
					i+1, result.Topic, result.Partition, result.Offset)
			}
		}
		if len(batchResult.Results) > 3 {
			fmt.Printf("      ... 还有 %d 条消息\n", len(batchResult.Results)-3)
		}
	}

	// 3. 性能对比演示
	fmt.Println("\n⚡ 性能对比演示（缓存 vs 非缓存）...")

	// 创建非缓存版本的生产者
	nonCacheConfig := dbmq.ProducerConfig{
		DB:                  db,
		Redis:               redisClient,
		NotificationEnabled: true,
		OffsetCacheEnabled:  false, // 禁用缓存
	}

	nonCacheProducer, err := dbmq.NewProducer(nonCacheConfig)
	if err != nil {
		log.Printf("❌ 创建非缓存生产者失败: %v", err)
		return
	}
	defer nonCacheProducer.Close()

	// 准备测试消息
	testMessages := make([]*dbmq.ProducerMessage, 5)
	for i := 0; i < 5; i++ {
		testMessages[i] = &dbmq.ProducerMessage{
			Topic: "performance_comparison",
			Key:   []byte(fmt.Sprintf("perf_key_%d", i)),
			Value: []byte(fmt.Sprintf("性能测试消息 #%d", i+1)),
		}
	}

	// 测试非缓存版本
	fmt.Println("   🐌 测试非缓存版本...")
	startTime = time.Now()
	_, err = nonCacheProducer.SendBatch(ctx, testMessages)
	nonCacheDuration := time.Since(startTime)

	if err != nil {
		log.Printf("❌ 非缓存批量发送失败: %v", err)
	} else {
		fmt.Printf("   ✅ 非缓存版本耗时: %v\n", nonCacheDuration)
	}

	// 测试缓存版本
	fmt.Println("   🚀 测试缓存版本...")
	startTime = time.Now()
	_, err = producer.SendBatch(ctx, testMessages)
	cacheDuration := time.Since(startTime)

	if err != nil {
		log.Printf("❌ 缓存批量发送失败: %v", err)
	} else {
		fmt.Printf("   ✅ 缓存版本耗时: %v\n", cacheDuration)

		// 计算性能提升
		if nonCacheDuration > 0 && cacheDuration > 0 {
			improvement := float64(nonCacheDuration-cacheDuration) / float64(nonCacheDuration) * 100
			fmt.Printf("   📈 性能提升: %.1f%%\n", improvement)
		}
	}

	fmt.Println("\n🎉 批量发送和Redis缓存演示完成！")
}
