package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/pkg"
)

func main() {
	fmt.Println("🚀 启动DBMQ监控指标和REST API演示程序")

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
		log.Fatalf("❌ 数据库初始化失败: %v", err)
	}

	// 创建监控指标客户端
	metricsClient, err := pkg.NewMetricsClient(pkg.MetricsConfig{
		DB: dbClient,
	})
	if err != nil {
		log.Fatalf("❌ 监控指标客户端创建失败: %v", err)
	}

	// 创建REST API服务器
	restServer, err := pkg.NewRestAPIServer(pkg.RestAPIConfig{
		DB:     dbClient,
		Port:   8080,
		Host:   "0.0.0.0", // 监听所有接口
		Prefix: "/api/v1",
	})
	if err != nil {
		log.Fatalf("❌ REST API服务器创建失败: %v", err)
	}

	// 演示监控指标功能
	go demonstrateMetrics(metricsClient)

	// 启动REST API服务器
	go func() {
		fmt.Println("🌐 REST API服务器启动在 http://localhost:8080")
		fmt.Println("📊 访问以下端点查看监控数据:")
		fmt.Println("  - 健康检查: http://localhost:8080/api/v1/health")
		fmt.Println("  - 集群指标: http://localhost:8080/api/v1/clusters/dbmq-cluster/metrics")
		fmt.Println("  - Topic列表: http://localhost:8080/api/v1/clusters/dbmq-cluster/topics")
		fmt.Println("  - 消费组列表: http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups")
		fmt.Println("  - Broker信息: http://localhost:8080/api/v1/clusters/dbmq-cluster/brokers")
		fmt.Println("  - DBMQ统计: http://localhost:8080/api/v1/dbmq/stats")
		fmt.Println()
		fmt.Println("🔧 兼容Kafka UI的接口:")
		fmt.Println("  - Kafka REST Proxy风格: http://localhost:8080/api/v1/topics")
		fmt.Println("  - Spring Boot Actuator: http://localhost:8080/api/v1/actuator/health")
		fmt.Println()

		if err := restServer.Start(); err != nil {
			log.Printf("❌ REST API服务器启动失败: %v", err)
		}
	}()

	// 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\n🛑 正在优雅关闭服务器...")

	// 停止REST API服务器
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := restServer.Stop(ctx); err != nil {
		log.Printf("❌ 服务器关闭失败: %v", err)
	} else {
		fmt.Println("✅ 服务器已优雅关闭")
	}
}

// demonstrateMetrics 演示监控指标功能
func demonstrateMetrics(metricsClient *pkg.MetricsClient) {
	ctx := context.Background()

	// 等待一段时间让服务器启动
	time.Sleep(2 * time.Second)

	fmt.Println("📈 开始演示监控指标功能...")

	// 每30秒输出一次监控指标
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	// 立即执行一次
	printMetrics(ctx, metricsClient)

	for {
		select {
		case <-ticker.C:
			printMetrics(ctx, metricsClient)
		}
	}
}

// printMetrics 打印监控指标
func printMetrics(ctx context.Context, metricsClient *pkg.MetricsClient) {
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Printf("📊 DBMQ监控指标报告 - %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 60))

	// 集群指标
	clusterMetrics, err := metricsClient.GetClusterMetrics(ctx)
	if err != nil {
		fmt.Printf("❌ 获取集群指标失败: %v\n", err)
	} else {
		fmt.Printf("🏢 集群指标:\n")
		fmt.Printf("   集群ID: %s\n", clusterMetrics.ClusterID)
		fmt.Printf("   Broker数量: %d\n", clusterMetrics.BrokerCount)
		fmt.Printf("   Topic数量: %d\n", clusterMetrics.TopicCount)
		fmt.Printf("   分区数量: %d\n", clusterMetrics.PartitionCount)
		fmt.Printf("   消息总数: %d\n", clusterMetrics.MessageCount)
		fmt.Printf("   消费组数量: %d\n", clusterMetrics.ConsumerGroups)
		fmt.Printf("   活跃消费者: %d\n", clusterMetrics.ActiveConsumers)
	}

	// Broker指标
	brokerMetrics, err := metricsClient.GetBrokerMetrics(ctx)
	if err != nil {
		fmt.Printf("❌ 获取Broker指标失败: %v\n", err)
	} else {
		fmt.Printf("\n🖥️  Broker指标:\n")
		fmt.Printf("   Broker ID: %d\n", brokerMetrics.BrokerID)
		fmt.Printf("   地址: %s:%d\n", brokerMetrics.Host, brokerMetrics.Port)
		fmt.Printf("   是否控制器: %t\n", brokerMetrics.IsController)
		fmt.Printf("   运行时间: %d秒\n", brokerMetrics.Uptime)
		fmt.Printf("   版本: %s\n", brokerMetrics.Version)
	}

	// Topic指标
	topicsMetrics, err := metricsClient.GetAllTopicsMetrics(ctx)
	if err != nil {
		fmt.Printf("❌ 获取Topic指标失败: %v\n", err)
	} else {
		fmt.Printf("\n📁 Topic指标:\n")
		if len(topicsMetrics) == 0 {
			fmt.Printf("   暂无Topic\n")
		} else {
			for _, topic := range topicsMetrics {
				fmt.Printf("   📄 %s: %d分区, %d消息, %.2fKB\n",
					topic.TopicName,
					topic.PartitionCount,
					topic.MessageCount,
					float64(topic.SizeBytes)/1024)
			}
		}
	}

	// 消费组指标
	groupsMetrics, err := metricsClient.GetAllConsumerGroupsMetrics(ctx)
	if err != nil {
		fmt.Printf("❌ 获取消费组指标失败: %v\n", err)
	} else {
		fmt.Printf("\n👥 消费组指标:\n")
		if len(groupsMetrics) == 0 {
			fmt.Printf("   暂无活跃消费组\n")
		} else {
			for _, group := range groupsMetrics {
				fmt.Printf("   🔗 %s: %s, %d成员, 延迟%d\n",
					group.GroupID,
					group.State,
					len(group.Members),
					group.Lag)
			}
		}
	}

	fmt.Println(strings.Repeat("=", 60))
}
