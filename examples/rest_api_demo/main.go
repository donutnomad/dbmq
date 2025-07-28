package main

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"log"
	"os"
	"os/signal"
	"strings"
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
	fmt.Println(strings.Repeat("=", 50))

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
		fmt.Println(strings.Repeat("=", 50))

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
func initDatabase() (interfaces.DB, error) {
	// 简单的数据库连接字符串
	dsn := "root:123456@tcp(localhost:3306)/dbmq_demo?charset=utf8mb4&parseTime=True&loc=Local"

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
