package db

import (
	"fmt"

	"github.com/donutnomad/dbmq/internal/dal"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	// mq_topics 表定义
	// 存储Topic元数据，包括分区数量和配置信息
	// Topic是消息队列的基本组织单位
	mqTopicsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_topics`" + ` (
  ` + "`topic_name`" + ` VARCHAR(255) NOT NULL PRIMARY KEY COMMENT 'Topic名称',
  ` + "`partition_count`" + ` INT UNSIGNED NOT NULL COMMENT '分区数量，创建后不可修改',
  ` + "`configs`" + ` JSON NULL COMMENT 'Topic级别配置, e.g. {"retention_ms": 604800000}',
  ` + "`created_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB COMMENT='Topic元数据表';`

	// mq_messages 表定义
	// 系统中最重要的表，存储所有消息数据
	// 使用复合索引优化消费查询性能
	// 使用全局ID作为主键，消除per_partition_offset带来的死锁问题
	mqMessagesSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_messages`" + ` (
  ` + "`id`" + ` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '全局唯一ID，用于消息排序和消费',
  ` + "`topic`" + ` VARCHAR(255) NOT NULL,
  ` + "`partition`" + ` INT UNSIGNED NOT NULL,
  ` + "`message_key`" + ` VARCHAR(255) NULL COMMENT '消息的业务Key, 用于分区策略',
  ` + "`headers`" + ` JSON NULL,
  ` + "`body`" + ` LONGBLOB NOT NULL,
  ` + "`created_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX ` + "`idx_consume_pull`" + ` (` + "`topic`" + `, ` + "`partition`" + `, ` + "`id`" + `),
  INDEX ` + "`idx_created_at`" + ` (` + "`created_at`" + `)
) ENGINE=InnoDB COMMENT='消息持久化日志表';`

	// mq_consumer_group_generations 表定义
	// 存储消费组的代际信息，是重新均衡机制的核心
	// 每次重新均衡时代际ID递增，用于隔离不同代际的消费者
	mqConsumerGroupGenerationsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_consumer_group_generations`" + ` (
  ` + "`group_id`" + ` VARCHAR(255) NOT NULL PRIMARY KEY COMMENT '消费组ID',
  ` + "`generation_id`" + ` INT UNSIGNED NOT NULL COMMENT '代际ID, 每次再均衡时加一',
  ` + "`protocol_type`" + ` VARCHAR(50) NOT NULL DEFAULT 'consumer',
  ` + "`leader_id`" + ` VARCHAR(255) NULL,
  ` + "`updated_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB COMMENT='消费组代际与元数据表';`

	// mq_consumer_heartbeats 表定义
	// 存储消费者心跳、分区分配和订阅信息
	// 协调器通过此表判断消费者存活状态和进行分区分配
	// last_heartbeat索引是性能关键，用于快速找到超时的消费者
	mqConsumerHeartbeatsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_consumer_heartbeats`" + ` (
  ` + "`group_id`" + ` VARCHAR(255) NOT NULL,
  ` + "`consumer_id`" + ` VARCHAR(255) NOT NULL COMMENT '消费者唯一ID (e.g., UUID)',
  ` + "`generation_id`" + ` INT UNSIGNED NOT NULL COMMENT '消费者当前所属的代际ID',
  ` + "`subscribed_topics`" + ` JSON NOT NULL COMMENT '订阅的Topic列表, e.g. ["topic-A", "topic-B"]',
  ` + "`assigned_partitions`" + ` JSON NOT NULL COMMENT '被分配的分区, e.g. {"topic-A": [0, 2]}',
  ` + "`last_heartbeat`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`group_id`" + `, ` + "`consumer_id`" + `),
  INDEX ` + "`idx_last_heartbeat`" + ` (` + "`last_heartbeat`" + `)
) ENGINE=InnoDB COMMENT='消费者心跳与分区分配表';`

	// mq_consumer_group_offsets 表定义
	// 存储消费组的偏移量提交记录
	// 实现"至少一次"消费语义的关键表
	// 使用代际隔离防止旧代际消费者覆盖新代际的偏移量
	// committed_offset现在存储的是全局消息ID
	mqConsumerGroupOffsetsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_consumer_group_offsets`" + ` (
  ` + "`group_id`" + ` VARCHAR(255) NOT NULL,
  ` + "`topic`" + ` VARCHAR(255) NOT NULL,
  ` + "`partition`" + ` INT UNSIGNED NOT NULL,
  ` + "`committed_offset`" + ` BIGINT NOT NULL COMMENT '已提交的最大消息ID',
  ` + "`generation_id`" + ` INT UNSIGNED NOT NULL COMMENT '提交该偏移量时所属的代际ID',
  ` + "`metadata`" + ` VARCHAR(255) NULL,
  ` + "`updated_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`group_id`" + `, ` + "`topic`" + `, ` + "`partition`" + `)
) ENGINE=InnoDB COMMENT='消费组偏移量提交表';`
)

// schemas 包含所有需要创建的表定义
// 按照依赖关系排序，确保创建顺序正确
var schemas = []string{
	mqTopicsSchema,
	mqMessagesSchema,
	mqConsumerGroupGenerationsSchema,
	mqConsumerHeartbeatsSchema,
	mqConsumerGroupOffsetsSchema,
}

// CreateDatabaseIfNotExists 创建数据库（如果不存在）
// 此函数打开临时连接到MySQL服务器来执行此操作
// 设计为在启动时调用一次
func CreateDatabaseIfNotExists(config MySQLConfig) error {
	// 不包含数据库名的DSN，用于连接到MySQL服务器
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // 静默模式，避免日志干扰
	})
	if err != nil {
		return fmt.Errorf("failed to connect to mysql server for db creation: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB for db creation: %w", err)
	}
	defer func() {
		_ = sqlDB.Close()
	}()

	// 创建数据库，使用UTF8MB4字符集和Unicode排序规则
	exec := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", config.DBName)
	if err := db.Exec(exec).Error; err != nil {
		return fmt.Errorf("failed to create database %s: %w", config.DBName, err)
	}

	return nil
}

// ApplySchemas 在给定的数据库连接上创建表
// 按照预定义的顺序执行所有表创建语句
func ApplySchemas(db dal.DB) error {
	for _, schema := range schemas {
		if err := db.Exec(schema).Error; err != nil {
			return fmt.Errorf("failed to apply schema: %w", err)
		}
	}
	return nil
}
