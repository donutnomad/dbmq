// Package examples 高级手动提交示例
// 展示如何精确控制偏移量提交，包括单个消息提交和批量提交
package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/pkg"
	"github.com/donutnomad/dbmq/pkg/types"
)

// AdvancedCommitDemo 高级手动提交演示
func AdvancedCommitDemo() {
	fmt.Println("🚀 高级手动提交演示")
	fmt.Println("💡 展示精确控制偏移量提交的不同方式：")
	fmt.Println("   1. 单个消息提交")
	fmt.Println("   2. 批量指定偏移量提交")
	fmt.Println("   3. 条件性提交（处理成功才提交）")
	fmt.Println()

	// 初始化数据库连接
	mysqlConfig := db.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
		DBName:   "dbmq_demo",
	}

	dbClient, err := db.InitMySQL(mysqlConfig)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}

	// 创建消费者（手动提交模式）
	consumer, err := pkg.NewConsumer(pkg.ConsumerConfig{
		DB:               dbClient,
		GroupID:          "高级手动提交演示组",
		EnableAutoCommit: false,
		Topics:           []string{"创建订单"},
	})
	if err != nil {
		log.Fatalf("❌ 创建消费者失败: %v", err)
	}
	defer consumer.Close()

	// 订阅主题
	err = consumer.SubscribeTopics("创建订单")
	if err != nil {
		log.Fatalf("❌ 订阅主题失败: %v", err)
	}

	fmt.Println("✅ 消费者创建成功，开始演示...")
	fmt.Println()

	// 演示1：单个消息提交
	fmt.Println("📝 演示1：单个消息提交")
	demonstrateSingleMessageCommit(consumer)

	// 演示2：批量指定偏移量提交
	fmt.Println("📝 演示2：批量指定偏移量提交")
	demonstrateBatchCommit(consumer)

	// 演示3：条件性提交
	fmt.Println("📝 演示3：条件性提交（处理成功才提交）")
	demonstrateConditionalCommit(consumer)

	fmt.Println("🏁 高级手动提交演示完成！")
}

// demonstrateSingleMessageCommit 演示单个消息提交
func demonstrateSingleMessageCommit(consumer *pkg.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("   正在拉取消息...")
	messages, err := consumer.Poll(ctx, 2*time.Second)
	if err != nil {
		fmt.Printf("   ⚠️  拉取消息失败或无消息: %v\n", err)
		return
	}

	if len(messages) == 0 {
		fmt.Println("   ℹ️  当前无消息可消费")
		return
	}

	// 逐个处理和提交消息
	for _, msg := range messages {
		fmt.Printf("   🔄 处理消息 ID: %d\n", msg.Offset)

		// 模拟消息处理
		time.Sleep(100 * time.Millisecond)

		// 提交单个消息的偏移量
		if err := consumer.CommitMessage(msg); err != nil {
			fmt.Printf("   ❌ 提交消息 %d 失败: %v\n", msg.Offset, err)
		} else {
			fmt.Printf("   ✅ 成功提交消息 %d 的偏移量\n", msg.Offset)
		}
	}
	fmt.Println()
}

// demonstrateBatchCommit 演示批量指定偏移量提交
func demonstrateBatchCommit(consumer *pkg.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("   正在拉取一批消息...")
	messages, err := consumer.Poll(ctx, 2*time.Second)
	if err != nil {
		fmt.Printf("   ⚠️  拉取消息失败或无消息: %v\n", err)
		return
	}

	if len(messages) == 0 {
		fmt.Println("   ℹ️  当前无消息可消费")
		return
	}

	// 收集要提交的偏移量
	offsetsToCommit := make(map[types.PartitionInfo]int64)

	fmt.Printf("   📦 处理 %d 条消息...\n", len(messages))
	for _, msg := range messages {
		fmt.Printf("   🔄 处理消息 ID: %d\n", msg.Offset)

		// 模拟消息处理
		time.Sleep(50 * time.Millisecond)

		// 收集偏移量（这里我们假设所有消息都处理成功）
		partition := types.PartitionInfo{
			Topic:     msg.Topic,
			Partition: msg.Partition,
		}
		// 保存最大的偏移量（因为偏移量是递增的）
		if existingOffset, exists := offsetsToCommit[partition]; !exists || msg.Offset > existingOffset {
			offsetsToCommit[partition] = msg.Offset
		}
	}

	// 批量提交所有偏移量
	if len(offsetsToCommit) > 0 {
		fmt.Printf("   📊 批量提交 %d 个分区的偏移量...\n", len(offsetsToCommit))
		if err := consumer.CommitOffsets(offsetsToCommit); err != nil {
			fmt.Printf("   ❌ 批量提交失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功批量提交偏移量: %v\n", offsetsToCommit)
		}
	}
	fmt.Println()
}

// demonstrateConditionalCommit 演示条件性提交
func demonstrateConditionalCommit(consumer *pkg.Consumer) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fmt.Println("   正在拉取消息进行条件性处理...")
	messages, err := consumer.Poll(ctx, 2*time.Second)
	if err != nil {
		fmt.Printf("   ⚠️  拉取消息失败或无消息: %v\n", err)
		return
	}

	if len(messages) == 0 {
		fmt.Println("   ℹ️  当前无消息可消费")
		return
	}

	successfulOffsets := make(map[types.PartitionInfo]int64)

	for _, msg := range messages {
		fmt.Printf("   🔄 处理消息 ID: %d\n", msg.Offset)

		// 解析消息内容来决定是否处理成功
		var order OrderMessage
		err := json.Unmarshal(msg.Value, &order)
		if err != nil {
			fmt.Printf("   ❌ 消息 %d 解析失败，跳过提交: %v\n", msg.Offset, err)
			continue
		}

		// 模拟业务逻辑：金额大于100的订单才算处理成功
		if order.Amount > 100 {
			fmt.Printf("   ✅ 消息 %d 处理成功 (订单: %s, 金额: %.2f)\n",
				msg.Offset, order.OrderID, order.Amount)

			// 记录成功处理的偏移量
			partition := types.PartitionInfo{
				Topic:     msg.Topic,
				Partition: msg.Partition,
			}
			successfulOffsets[partition] = msg.Offset
		} else {
			fmt.Printf("   ⚠️  消息 %d 处理失败 (订单: %s, 金额太小: %.2f)，不提交偏移量\n",
				msg.Offset, order.OrderID, order.Amount)
		}

		time.Sleep(100 * time.Millisecond)
	}

	// 只提交处理成功的消息偏移量
	if len(successfulOffsets) > 0 {
		fmt.Printf("   📊 提交 %d 个成功处理的消息偏移量...\n", len(successfulOffsets))
		if err := consumer.CommitOffsets(successfulOffsets); err != nil {
			fmt.Printf("   ❌ 条件性提交失败: %v\n", err)
		} else {
			fmt.Printf("   ✅ 成功提交处理成功的偏移量: %v\n", successfulOffsets)
		}
	} else {
		fmt.Println("   ℹ️  没有成功处理的消息，无需提交偏移量")
	}
	fmt.Println()
}
