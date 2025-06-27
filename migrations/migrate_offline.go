package main

import (
	"fmt"
	"log"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func main() {
	// 连接到数据库
	dsn := "root:CdESwVG3wwPWYw@tcp(localhost:3306)/dbmq_demo?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to connect to database:", err)
	}

	fmt.Println("正在添加 offline 相关字段...")

	// 添加 offline 字段
	err = db.Exec("ALTER TABLE `mq_consumer_heartbeats` ADD COLUMN `offline` BOOLEAN NOT NULL DEFAULT FALSE COMMENT '是否已下线：true=主动下线，false=在线或超时' AFTER `assigned_partitions`").Error
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate column name 'offline'") {
			fmt.Println("offline 字段已存在，跳过...")
		} else {
			fmt.Printf("添加 offline 字段时出错: %v\n", err)
		}
	} else {
		fmt.Println("✅ 成功添加 offline 字段")
	}

	// 添加 offline_at 字段
	err = db.Exec("ALTER TABLE `mq_consumer_heartbeats` ADD COLUMN `offline_at` TIMESTAMP(3) NULL COMMENT '下线时间，仅当offline=true时有效' AFTER `last_heartbeat`").Error
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate column name 'offline_at'") {
			fmt.Println("offline_at 字段已存在，跳过...")
		} else {
			fmt.Printf("添加 offline_at 字段时出错: %v\n", err)
		}
	} else {
		fmt.Println("✅ 成功添加 offline_at 字段")
	}

	// 添加索引
	err = db.Exec("ALTER TABLE `mq_consumer_heartbeats` ADD INDEX `idx_offline_status` (`offline`, `last_heartbeat`)").Error
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate key name 'idx_offline_status'") {
			fmt.Println("索引 idx_offline_status 已存在，跳过...")
		} else {
			fmt.Printf("添加索引时出错: %v\n", err)
		}
	} else {
		fmt.Println("✅ 成功添加 idx_offline_status 索引")
	}

	// 更新现有记录
	result := db.Exec("UPDATE `mq_consumer_heartbeats` SET `offline` = FALSE, `offline_at` = NULL")
	if result.Error != nil {
		fmt.Printf("更新现有记录时出错: %v\n", result.Error)
	} else {
		fmt.Printf("✅ 更新了 %d 条现有记录为在线状态\n", result.RowsAffected)
	}

	fmt.Println("\n🎉 数据库迁移完成！")
	fmt.Println("- offline 字段：BOOLEAN，默认 FALSE")
	fmt.Println("- offline_at 字段：TIMESTAMP(3)，可为 NULL")
	fmt.Println("- idx_offline_status 索引：提高查询性能")
}
