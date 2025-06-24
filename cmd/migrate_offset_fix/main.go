package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/donutnomad/dbmq/internal/db"
)

func main() {
	// 命令行参数
	var (
		host     = flag.String("host", "localhost", "MySQL主机地址")
		port     = flag.Int("port", 3306, "MySQL端口")
		user     = flag.String("user", "root", "MySQL用户名")
		password = flag.String("password", "", "MySQL密码")
		dbname   = flag.String("dbname", "dbmq_demo", "数据库名称")
		validate = flag.Bool("validate", false, "仅验证offset正确性，不执行迁移")
		stats    = flag.Bool("stats", false, "显示offset统计信息")
	)
	flag.Parse()

	if *password == "" {
		fmt.Print("请输入MySQL密码: ")
		fmt.Scanln(password)
	}

	fmt.Println("🚀 DBMQ Offset修复工具")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("连接信息: %s@%s:%d/%s\n", *user, *host, *port, *dbname)
	fmt.Println(strings.Repeat("=", 50))

	// 初始化数据库连接
	mysqlConfig := db.MySQLConfig{
		Host:     *host,
		Port:     *port,
		User:     *user,
		Password: *password,
		DBName:   *dbname,
	}

	dbClient, err := db.InitMySQL(mysqlConfig)
	if err != nil {
		log.Fatalf("❌ 连接数据库失败: %v", err)
	}

	// 根据命令行参数执行相应操作
	if *validate {
		fmt.Println("🔍 开始验证offset正确性...")
		if err := db.ValidatePerPartitionOffsets(dbClient); err != nil {
			log.Fatalf("❌ 验证失败: %v", err)
		}
		fmt.Println("✅ 验证完成")
		return
	}

	if *stats {
		fmt.Println("📊 获取offset统计信息...")
		if err := db.GetOffsetStatistics(dbClient); err != nil {
			log.Fatalf("❌ 获取统计信息失败: %v", err)
		}
		return
	}

	// 执行迁移
	fmt.Println("⚠️  警告：此操作将修改数据库结构，请确保已备份数据")
	fmt.Print("是否继续？(y/N): ")
	var confirm string
	fmt.Scanln(&confirm)

	if confirm != "y" && confirm != "Y" {
		fmt.Println("❌ 操作已取消")
		os.Exit(0)
	}

	fmt.Println("🔄 开始执行offset修复迁移...")
	if err := db.MigrateToPerPartitionOffset(dbClient); err != nil {
		log.Fatalf("❌ 迁移失败: %v", err)
	}

	fmt.Println("🔍 验证迁移结果...")
	if err := db.ValidatePerPartitionOffsets(dbClient); err != nil {
		log.Printf("⚠️  验证警告: %v", err)
	}

	fmt.Println("📊 迁移后统计信息:")
	if err := db.GetOffsetStatistics(dbClient); err != nil {
		log.Printf("⚠️  获取统计信息失败: %v", err)
	}

	fmt.Println()
	fmt.Println("🎉 offset修复迁移完成！")
	fmt.Println("现在每个主题的分区都有独立的offset，从0开始计数")
	fmt.Println("请重新启动您的DBMQ应用程序以使用新的offset机制")
}
