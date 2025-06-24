// Package examples 展示如何使用DBMQ的AdminClient API
// 模仿Kafka AdminClient的使用方式
package examples

import (
	"context"
	"errors"
	"fmt"
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/db"
	"log"
	"time"
)

// AdminExample 展示AdminClient的基本用法
func AdminExample() {
	// 1. 初始化数据库连接
	mysqlConfig := db.MySQLConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "root",
		Password: "password",
		DBName:   "dbmq_example",
	}

	// 创建数据库（如果不存在）
	err := db.CreateDatabaseIfNotExists(mysqlConfig)
	if err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	// 初始化数据库连接
	dbClient, err := db.InitMySQL(mysqlConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// 应用数据库模式
	err = db.ApplySchemas(dbClient)
	if err != nil {
		log.Fatalf("Failed to apply schemas: %v", err)
	}

	// 2. 创建AdminClient
	admin, err := dbmq.NewAdminClient(dbmq.AdminConfig{
		DB: dbClient,
	})
	if err != nil {
		log.Fatalf("Failed to create admin client: %v", err)
	}
	defer admin.Close()

	ctx := context.Background()

	// 3. 创建Topic - 基本用法
	fmt.Println("=== 创建基本Topic ===")
	basicTopicReq := dbmq.NewTopicRequest{
		Name:          "orders",
		NumPartitions: 3,
	}

	err = admin.CreateTopic(ctx, basicTopicReq)
	if err != nil {
		log.Printf("Failed to create basic topic: %v", err)
	} else {
		fmt.Printf("Successfully created topic: %s with %d partitions\n",
			basicTopicReq.Name, basicTopicReq.NumPartitions)
	}

	// 4. 创建带配置的Topic
	fmt.Println("\n=== 创建带配置的Topic ===")
	retentionHours := 48
	retentionMs := int64(7 * 24 * 60 * 60 * 1000) // 7天

	configTopicReq := dbmq.NewTopicRequest{
		Name:          "user-events",
		NumPartitions: 5,
		Config: &dbmq.TopicConfig{
			RetentionHours: &retentionHours,
			RetentionMs:    &retentionMs,
			CleanupPolicy:  "delete",
		},
	}

	err = admin.CreateTopic(ctx, configTopicReq)
	if err != nil {
		log.Printf("Failed to create configured topic: %v", err)
	} else {
		fmt.Printf("Successfully created configured topic: %s\n", configTopicReq.Name)
	}

	// 5. 批量创建Topic
	fmt.Println("\n=== 批量创建Topic ===")
	batchRequests := []dbmq.NewTopicRequest{
		{
			Name:          "notifications",
			NumPartitions: 2,
		},
		{
			Name:          "analytics",
			NumPartitions: 4,
		},
		{
			Name:          "logs",
			NumPartitions: 1,
		},
	}

	result := admin.CreateTopics(ctx, batchRequests)
	for _, topicResult := range result.Results {
		if topicResult.Error != nil {
			log.Printf("Failed to create topic %s: %v", topicResult.Name, topicResult.Error)
		} else {
			fmt.Printf("Successfully created topic: %s\n", topicResult.Name)
		}
	}

	// 6. 列出所有Topic
	fmt.Println("\n=== 列出所有Topic ===")
	topics, err := admin.ListTopics(ctx)
	if err != nil {
		log.Printf("Failed to list topics: %v", err)
	} else {
		fmt.Printf("Found %d topics:\n", len(topics))
		for _, topic := range topics {
			fmt.Printf("  - %s\n", topic)
		}
	}

	// 7. 描述Topic
	fmt.Println("\n=== 描述Topic ===")
	descriptions, err := admin.DescribeTopics(ctx, []string{"orders", "user-events"})
	if err != nil {
		log.Printf("Failed to describe topics: %v", err)
	} else {
		for name, desc := range descriptions {
			fmt.Printf("Topic: %s\n", name)
			fmt.Printf("  Partitions: %d\n", desc.NumPartitions)
			fmt.Printf("  Created: %s\n", desc.CreatedAt.Format(time.RFC3339))
			if desc.Config != nil {
				if desc.Config.RetentionHours != nil {
					fmt.Printf("  Retention Hours: %d\n", *desc.Config.RetentionHours)
				}
				if desc.Config.RetentionMs != nil {
					fmt.Printf("  Retention Ms: %d\n", *desc.Config.RetentionMs)
				}
				if desc.Config.CleanupPolicy != "" {
					fmt.Printf("  Cleanup Policy: %s\n", desc.Config.CleanupPolicy)
				}
			}
			fmt.Println()
		}
	}

	// 8. 验证Topic创建（仅验证，不实际创建）
	fmt.Println("\n=== 验证Topic创建 ===")
	validateReq := dbmq.NewTopicRequest{
		Name:          "temp-topic",
		NumPartitions: 1,
		ValidateOnly:  true,
	}

	validateResult := admin.CreateTopics(ctx, []dbmq.NewTopicRequest{validateReq})
	if validateResult.Results[0].Error != nil {
		log.Printf("Validation failed: %v", validateResult.Results[0].Error)
	} else {
		fmt.Printf("Topic validation passed for: %s\n", validateReq.Name)
	}

	// 9. 删除Topic
	fmt.Println("\n=== 删除Topic ===")
	err = admin.DeleteTopics(ctx, []string{"logs"})
	if err != nil {
		log.Printf("Failed to delete topic: %v", err)
	} else {
		fmt.Println("Successfully deleted topic: logs")
	}

	// 10. 最终列出所有Topic
	fmt.Println("\n=== 最终Topic列表 ===")
	finalTopics, err := admin.ListTopics(ctx)
	if err != nil {
		log.Printf("Failed to list final topics: %v", err)
	} else {
		fmt.Printf("Final topic count: %d\n", len(finalTopics))
		for _, topic := range finalTopics {
			fmt.Printf("  - %s\n", topic)
		}
	}
}

// CreateTopicIfNotExists 创建Topic，如果已存在则忽略
// 这模仿了Kafka命令行工具的 --if-not-exists 行为
func CreateTopicIfNotExists(admin *dbmq.AdminClient, req dbmq.NewTopicRequest) error {
	err := admin.CreateTopic(context.Background(), req)
	if err != nil {
		var topicExistsErr *dbmq.ErrTopicAlreadyExists
		if errors.As(err, &topicExistsErr) {
			fmt.Printf("Topic %s 已存在，跳过创建\n", req.Name)
			return nil // 忽略已存在的错误
		}
		return fmt.Errorf("创建Topic失败: %w", err)
	}
	fmt.Printf("成功创建Topic: %s\n", req.Name)
	return nil
}

// ErrorHandlingExample 展示各种错误处理场景
func ErrorHandlingExample() {
	// 假设已经初始化了AdminClient
	// admin, err := pkg.NewAdminClient(...)

	fmt.Println("=== 错误处理示例 ===")

	// 示例1: 处理Topic已存在错误
	// req := pkg.NewTopicRequest{
	//     Name:          "existing-topic",
	//     NumPartitions: 3,
	// }

	// 第一次创建 - 成功
	// err := admin.CreateTopic(context.Background(), req)

	// 第二次创建 - 会得到ErrTopicAlreadyExists
	// err = admin.CreateTopic(context.Background(), req)
	// if err != nil {
	//     var topicExistsErr *ErrTopicAlreadyExists
	//     if errors.As(err, &topicExistsErr) {
	//         fmt.Printf("Topic %s 已存在，可以安全忽略\n", topicExistsErr.TopicName)
	//     }
	// }

	// 示例2: 批量创建时的错误处理
	// requests := []pkg.NewTopicRequest{
	//     {Name: "topic1", NumPartitions: 2},
	//     {Name: "existing-topic", NumPartitions: 3}, // 这个会失败
	//     {Name: "topic3", NumPartitions: 1},
	// }

	// result := admin.CreateTopics(context.Background(), requests)
	// for _, topicResult := range result.Results {
	//     if topicResult.Error != nil {
	//         switch err := topicResult.Error.(type) {
	//         case *ErrTopicAlreadyExists:
	//             fmt.Printf("Topic %s 已存在，跳过\n", err.TopicName)
	//         default:
	//             fmt.Printf("创建Topic %s 失败: %v\n", topicResult.Name, err)
	//         }
	//     } else {
	//         fmt.Printf("成功创建Topic: %s\n", topicResult.Name)
	//     }
	// }
}

// KafkaStyleExample 展示更接近Kafka风格的用法
func KafkaStyleExample() {
	// 初始化（省略数据库连接代码，参考上面的例子）
	// ...

	fmt.Println("=== Kafka风格的Topic管理 ===")

	// 模仿Kafka的Properties配置方式
	// 在实际使用中，这些配置可以从配置文件或环境变量读取
	configs := map[string]interface{}{
		"retention.ms":    604800000, // 7天
		"cleanup.policy":  "delete",
		"partition.count": 3,
	}

	fmt.Printf("Topic配置: %+v\n", configs)

	// 这种方式更接近Kafka的AdminClient.createTopics()
	// 可以进一步扩展以支持更多Kafka兼容的配置选项
}
