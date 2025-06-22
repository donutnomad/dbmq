package db

import (
	"fmt"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const (
	mqTopicsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_topics`" + ` (
  ` + "`topic_name`" + ` VARCHAR(255) NOT NULL PRIMARY KEY COMMENT 'Topic名称',
  ` + "`partition_count`" + ` INT UNSIGNED NOT NULL COMMENT '分区数量，创建后不可修改',
  ` + "`configs`" + ` JSON NULL COMMENT 'Topic级别配置, e.g. {"retention_ms": 604800000}',
  ` + "`created_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB COMMENT='Topic元数据表';`

	mqMessagesSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_messages`" + ` (
  ` + "`id`" + ` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '全局唯一ID, 作为分区的Offset使用',
  ` + "`topic`" + ` VARCHAR(255) NOT NULL,
  ` + "`partition`" + ` INT UNSIGNED NOT NULL,
  ` + "`message_key`" + ` VARCHAR(255) NULL COMMENT '消息的业务Key, 用于分区策略',
  ` + "`headers`" + ` JSON NULL,
  ` + "`body`" + ` LONGBLOB NOT NULL,
  ` + "`created_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  INDEX ` + "`idx_consume_pull`" + ` (` + "`topic`" + `, ` + "`partition`" + `, ` + "`id`" + `),
  INDEX ` + "`idx_created_at`" + ` (` + "`created_at`" + `)
) ENGINE=InnoDB COMMENT='消息持久化日志表';`

	mqConsumerGroupGenerationsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_consumer_group_generations`" + ` (
  ` + "`group_id`" + ` VARCHAR(255) NOT NULL PRIMARY KEY COMMENT '消费组ID',
  ` + "`generation_id`" + ` INT UNSIGNED NOT NULL COMMENT '代际ID, 每次再均衡时加一',
  ` + "`protocol_type`" + ` VARCHAR(50) NOT NULL DEFAULT 'consumer',
  ` + "`leader_id`" + ` VARCHAR(255) NULL,
  ` + "`updated_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB COMMENT='消费组代际与元数据表';`

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

	mqConsumerGroupOffsetsSchema = `
CREATE TABLE IF NOT EXISTS ` + "`mq_consumer_group_offsets`" + ` (
  ` + "`group_id`" + ` VARCHAR(255) NOT NULL,
  ` + "`topic`" + ` VARCHAR(255) NOT NULL,
  ` + "`partition`" + ` INT UNSIGNED NOT NULL,
  ` + "`committed_offset`" + ` BIGINT NOT NULL,
  ` + "`generation_id`" + ` INT UNSIGNED NOT NULL COMMENT '提交该偏移量时所属的代际ID',
  ` + "`metadata`" + ` VARCHAR(255) NULL,
  ` + "`updated_at`" + ` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (` + "`group_id`" + `, ` + "`topic`" + `, ` + "`partition`" + `)
) ENGINE=InnoDB COMMENT='消费组偏移量提交表';`
)

var schemas = []string{
	mqTopicsSchema,
	mqMessagesSchema,
	mqConsumerGroupGenerationsSchema,
	mqConsumerHeartbeatsSchema,
	mqConsumerGroupOffsetsSchema,
}

// CreateDatabaseIfNotExists creates the database. This function opens a temporary
// connection to the MySQL server to do so. It's designed to be called once at startup.
func CreateDatabaseIfNotExists(config MySQLConfig) error {
	// DSN without database name to connect to the MySQL server
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
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

	// Create the database
	exec := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", config.DBName)
	if err := db.Exec(exec).Error; err != nil {
		return fmt.Errorf("failed to create database %s: %w", config.DBName, err)
	}

	return nil
}

// ApplySchemas creates the tables on the given database connection.
func ApplySchemas(db *gorm.DB) error {
	for _, schema := range schemas {
		if err := db.Exec(schema).Error; err != nil {
			return fmt.Errorf("failed to apply schema: %w", err)
		}
	}
	return nil
}
