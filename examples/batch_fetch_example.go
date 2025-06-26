package examples

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/donutnomad/dbmq/internal/dal"
	"github.com/donutnomad/dbmq/types"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// BatchFetchExample 演示批量获取消息的功能
func BatchFetchExample() {
	// 连接数据库
	dsn := "root:password@tcp(localhost:3306)/dbmq_example?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	ctx := context.Background()

	// 示例：准备一些测试数据
	fmt.Println("准备测试数据...")

	// 插入一些示例消息
	messages := []types.Message{
		{Topic: "orders", Partition: 0, Body: []byte("订单1"), Headers: []byte("{}")},
		{Topic: "orders", Partition: 1, Body: []byte("订单2"), Headers: []byte("{}")},
		{Topic: "users", Partition: 0, Body: []byte("用户1"), Headers: []byte("{}")},
		{Topic: "users", Partition: 1, Body: []byte("用户2"), Headers: []byte("{}")},
		{Topic: "orders", Partition: 0, Body: []byte("订单3"), Headers: []byte("{}")},
	}

	for i := range messages {
		if err := dal.CreateMessage(ctx, db, messages[i]); err != nil {
			log.Printf("Failed to create message: %v", err)
		}
	}

	fmt.Println("测试数据准备完成")

	// 演示批量获取
	fmt.Println("\n=== 演示批量获取功能 ===")

	// 准备批量请求
	batchRequests := []dal.PartitionRequest{
		{Topic: "orders", Partition: 0, Offset: 0, Limit: 10},
		{Topic: "orders", Partition: 1, Offset: 0, Limit: 10},
		{Topic: "users", Partition: 0, Offset: 0, Limit: 10},
		{Topic: "users", Partition: 1, Offset: 0, Limit: 10},
	}

	// 执行批量获取
	start := time.Now()
	allMessages, err := dal.NewMqDao(db).FetchMessagesBatch(ctx, batchRequests)
	duration := time.Since(start)

	if err != nil {
		log.Fatalf("批量获取失败: %v", err)
	}

	fmt.Printf("批量获取完成，耗时: %v\n", duration)
	fmt.Printf("获取到 %d 条消息:\n", len(allMessages))

	// 显示获取的消息
	for _, msg := range allMessages {
		fmt.Printf("  ID: %d, Topic: %s, Partition: %d, Body: %s\n",
			msg.ID, msg.Topic, msg.Partition, string(msg.Body))
	}

	// 对比：使用单独查询的方式
	fmt.Println("\n=== 对比：单独查询方式 ===")

	start = time.Now()
	var individualMessages []types.Message

	for _, req := range batchRequests {
		messages, err := dal.NewMqDao(db).FetchMessages(ctx, req.Topic, req.Partition, req.Offset, req.Limit)
		if err != nil {
			log.Printf("单独查询失败 %s:%d: %v", req.Topic, req.Partition, err)
			continue
		}
		individualMessages = append(individualMessages, messages...)
	}

	individualDuration := time.Since(start)

	fmt.Printf("单独查询完成，耗时: %v\n", individualDuration)
	fmt.Printf("获取到 %d 条消息\n", len(individualMessages))

	// 性能对比
	fmt.Printf("\n=== 性能对比 ===\n")
	fmt.Printf("批量查询耗时: %v\n", duration)
	fmt.Printf("单独查询耗时: %v\n", individualDuration)
	if individualDuration > duration {
		improvement := float64(individualDuration-duration) / float64(individualDuration) * 100
		fmt.Printf("批量查询性能提升: %.1f%%\n", improvement)
	}

	fmt.Println("\n示例完成！")
}
