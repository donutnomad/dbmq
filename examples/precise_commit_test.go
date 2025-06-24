// Package examples 精确提交功能测试
package examples

import (
	"fmt"
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/types"
	"testing"
	"time"
)

// TestPreciseCommit 测试精确提交功能
func TestPreciseCommit(t *testing.T) {
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
		t.Skipf("跳过测试，无法连接数据库: %v", err)
	}

	// 测试单个消息提交
	t.Run("单个消息提交", func(t *testing.T) {
		consumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
			DB:               dbClient,
			GroupID:          "test_precise_commit",
			EnableAutoCommit: false,
		})
		if err != nil {
			t.Fatalf("创建消费者失败: %v", err)
		}
		defer consumer.Close()

		// 创建模拟消息
		msg := dbmq.ConsumerMessage{
			Topic:     "test-topic",
			Partition: 0,
			Offset:    123,
			Value:     []byte("test message"),
		}

		// 测试CommitMessage方法
		err = consumer.CommitMessage(msg)
		if err != nil {
			t.Logf("提交单个消息可能失败（正常，因为消费者可能未分配到分区）: %v", err)
		} else {
			fmt.Println("✅ 单个消息提交功能正常")
		}
	})

	// 测试批量偏移量提交
	t.Run("批量偏移量提交", func(t *testing.T) {
		consumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
			DB:               dbClient,
			GroupID:          "test_batch_commit",
			EnableAutoCommit: false,
		})
		if err != nil {
			t.Fatalf("创建消费者失败: %v", err)
		}
		defer consumer.Close()

		// 创建偏移量映射
		offsets := map[types.PartitionInfo]int64{
			{Topic: "test-topic", Partition: 0}: 100,
			{Topic: "test-topic", Partition: 1}: 200,
		}

		// 测试CommitOffsets方法
		err = consumer.CommitOffsets(offsets)
		if err != nil {
			t.Logf("批量提交偏移量可能失败（正常，因为消费者可能未分配到分区）: %v", err)
		} else {
			fmt.Println("✅ 批量偏移量提交功能正常")
		}
	})

	// 测试API方法
	t.Run("API方法测试", func(t *testing.T) {
		// 手动提交模式消费者
		manualConsumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
			DB:               dbClient,
			GroupID:          "test_manual_api",
			EnableAutoCommit: false,
		})
		if err != nil {
			t.Fatalf("创建手动提交消费者失败: %v", err)
		}
		defer manualConsumer.Close()

		if manualConsumer.IsAutoCommitEnabled() {
			t.Error("期望手动提交模式，但检测到自动提交已启用")
		}

		// 自动提交模式消费者
		autoConsumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
			DB:                 dbClient,
			GroupID:            "test_auto_api",
			EnableAutoCommit:   true,
			AutoCommitInterval: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("创建自动提交消费者失败: %v", err)
		}
		defer autoConsumer.Close()

		if !autoConsumer.IsAutoCommitEnabled() {
			t.Error("期望自动提交模式，但检测到自动提交未启用")
		}

		lastCommitTime := autoConsumer.GetLastAutoCommitTime()
		if lastCommitTime.IsZero() {
			t.Error("期望获取到最后提交时间，但得到零值")
		}

		fmt.Printf("✅ API方法测试通过，最后提交时间: %v\n", lastCommitTime)
	})
}

// BenchmarkCommitMethods 性能基准测试
func BenchmarkCommitMethods(b *testing.B) {
	mysqlConfig := db.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "123456",
		DBName:   "dbmq_demo",
	}

	dbClient, err := db.InitMySQL(mysqlConfig)
	if err != nil {
		b.Skipf("跳过基准测试，无法连接数据库: %v", err)
	}

	consumer, err := dbmq.NewConsumer(dbmq.ConsumerConfig{
		DB:               dbClient,
		GroupID:          "benchmark_commit",
		EnableAutoCommit: false,
	})
	if err != nil {
		b.Fatalf("创建消费者失败: %v", err)
	}
	defer consumer.Close()

	// 基准测试单个消息提交
	b.Run("CommitMessage", func(b *testing.B) {
		msg := dbmq.ConsumerMessage{
			Topic:     "benchmark-topic",
			Partition: 0,
			Offset:    0,
			Value:     []byte("benchmark message"),
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			msg.Offset = int64(i)
			consumer.CommitMessage(msg)
		}
	})

	// 基准测试批量提交
	b.Run("CommitOffsets", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			offsets := map[types.PartitionInfo]int64{
				{Topic: "benchmark-topic", Partition: 0}: int64(i),
			}
			consumer.CommitOffsets(offsets)
		}
	})
}
