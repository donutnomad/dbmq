// Package examples 测试自动提交和手动提交功能
package examples

import (
	"fmt"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/pkg"
)

// TestCommitModes 测试不同的提交模式
func TestCommitModes(t *testing.T) {
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

	// 测试手动提交模式的消费者
	t.Run("手动提交模式", func(t *testing.T) {
		consumer, err := pkg.NewConsumer(pkg.ConsumerConfig{
			DB:               dbClient,
			GroupID:          "test_manual_commit",
			EnableAutoCommit: false, // 禁用自动提交
		})
		if err != nil {
			t.Fatalf("创建消费者失败: %v", err)
		}
		defer consumer.Close()

		if consumer.IsAutoCommitEnabled() {
			t.Error("期望手动提交模式，但检测到自动提交已启用")
		}

		fmt.Println("✅ 手动提交模式消费者创建成功")
	})

	// 测试自动提交模式的消费者
	t.Run("自动提交模式", func(t *testing.T) {
		consumer, err := pkg.NewConsumer(pkg.ConsumerConfig{
			DB:                 dbClient,
			GroupID:            "test_auto_commit",
			EnableAutoCommit:   true,
			AutoCommitInterval: 2 * time.Second,
		})
		if err != nil {
			t.Fatalf("创建消费者失败: %v", err)
		}
		defer consumer.Close()

		if !consumer.IsAutoCommitEnabled() {
			t.Error("期望自动提交模式，但检测到自动提交未启用")
		}

		fmt.Println("✅ 自动提交模式消费者创建成功")
		fmt.Printf("   最后一次自动提交时间: %v\n", consumer.GetLastAutoCommitTime())
	})

	// 测试默认配置
	t.Run("默认配置", func(t *testing.T) {
		consumer, err := pkg.NewConsumer(pkg.ConsumerConfig{
			DB:      dbClient,
			GroupID: "test_default",
			// 不设置自动提交配置，使用默认值
		})
		if err != nil {
			t.Fatalf("创建消费者失败: %v", err)
		}
		defer consumer.Close()

		// 默认应该是禁用自动提交的
		if consumer.IsAutoCommitEnabled() {
			t.Error("期望默认为手动提交模式，但检测到自动提交已启用")
		}

		fmt.Println("✅ 默认配置消费者创建成功（手动提交模式）")
	})
}
