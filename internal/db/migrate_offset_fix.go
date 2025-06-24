package db

import (
	"fmt"

	"gorm.io/gorm"
)

// MigrateToPerPartitionOffset 迁移现有数据以支持per_partition_offset
// 这个函数用于修复不同topic的offset混淆问题
func MigrateToPerPartitionOffset(db *gorm.DB) error {
	// 1. 首先检查是否已有per_partition_offset字段
	if db.Migrator().HasColumn(&struct{}{}, "per_partition_offset") {
		fmt.Println("✅ per_partition_offset字段已存在，跳过迁移")
		return nil
	}

	fmt.Println("🔄 开始offset修复迁移...")

	// 2. 添加新的字段
	fmt.Println("   添加per_partition_offset字段...")
	if err := db.Exec("ALTER TABLE mq_messages ADD COLUMN per_partition_offset BIGINT").Error; err != nil {
		return fmt.Errorf("添加per_partition_offset字段失败: %w", err)
	}

	// 3. 为现有数据计算正确的per_partition_offset
	fmt.Println("   为现有数据计算per_partition_offset...")
	updateSQL := `
UPDATE mq_messages m1 
SET per_partition_offset = (
    SELECT COUNT(*) - 1 
    FROM mq_messages m2 
    WHERE m2.topic = m1.topic 
      AND m2.partition = m1.partition 
      AND m2.id <= m1.id
)
`
	if err := db.Exec(updateSQL).Error; err != nil {
		return fmt.Errorf("更新现有数据的per_partition_offset失败: %w", err)
	}

	// 4. 设置NOT NULL约束
	fmt.Println("   设置NOT NULL约束...")
	if err := db.Exec("ALTER TABLE mq_messages MODIFY COLUMN per_partition_offset BIGINT NOT NULL").Error; err != nil {
		return fmt.Errorf("设置per_partition_offset NOT NULL约束失败: %w", err)
	}

	// 5. 添加唯一索引
	fmt.Println("   添加唯一索引...")
	if err := db.Exec("ALTER TABLE mq_messages ADD UNIQUE KEY uk_topic_partition_offset (topic, partition, per_partition_offset)").Error; err != nil {
		return fmt.Errorf("添加唯一索引失败: %w", err)
	}

	// 6. 更新消费索引
	fmt.Println("   更新消费索引...")

	// 检查索引是否存在
	var indexExists int
	if err := db.Raw("SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'mq_messages' AND index_name = 'idx_consume_pull'").Scan(&indexExists).Error; err != nil {
		fmt.Printf("   警告：检查索引是否存在失败: %v\n", err)
	} else if indexExists > 0 {
		if err := db.Exec("DROP INDEX idx_consume_pull ON mq_messages").Error; err != nil {
			fmt.Printf("   警告：删除旧索引失败: %v\n", err)
		}
	}

	if err := db.Exec("CREATE INDEX idx_consume_pull ON mq_messages (topic, partition, per_partition_offset)").Error; err != nil {
		return fmt.Errorf("创建新的消费索引失败: %w", err)
	}

	fmt.Println("✅ offset修复迁移完成")
	return nil
}

// ValidatePerPartitionOffsets 验证per_partition_offset的正确性
func ValidatePerPartitionOffsets(db *gorm.DB) error {
	fmt.Println("🔍 验证per_partition_offset的正确性...")

	// 检查是否有重复的(topic, partition, per_partition_offset)
	var duplicateCount int64
	err := db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT topic, partition, per_partition_offset, COUNT(*) as cnt
			FROM mq_messages 
			GROUP BY topic, partition, per_partition_offset
			HAVING COUNT(*) > 1
		) duplicates
	`).Scan(&duplicateCount).Error

	if err != nil {
		return fmt.Errorf("检查重复offset失败: %w", err)
	}

	if duplicateCount > 0 {
		return fmt.Errorf("发现 %d 个重复的offset，数据不一致", duplicateCount)
	}

	// 检查每个分区的offset是否连续
	var gapCount int64
	err = db.Raw(`
		SELECT COUNT(*) FROM (
			SELECT topic, partition, 
				per_partition_offset,
				LAG(per_partition_offset) OVER (PARTITION BY topic, partition ORDER BY per_partition_offset) as prev_offset
			FROM mq_messages
		) gaps
		WHERE prev_offset IS NOT NULL 
		  AND per_partition_offset != prev_offset + 1
	`).Scan(&gapCount).Error

	if err != nil {
		return fmt.Errorf("检查offset连续性失败: %w", err)
	}

	if gapCount > 0 {
		fmt.Printf("   警告：发现 %d 个offset间隙，这可能是由于消息删除导致的\n", gapCount)
	}

	fmt.Println("✅ per_partition_offset验证通过")
	return nil
}

// GetOffsetStatistics 获取offset统计信息
func GetOffsetStatistics(db *gorm.DB) error {
	fmt.Println("📊 offset统计信息:")

	// 获取每个分区的offset范围
	type PartitionStats struct {
		Topic     string `gorm:"column:topic"`
		Partition uint   `gorm:"column:partition"`
		MinOffset int64  `gorm:"column:min_offset"`
		MaxOffset int64  `gorm:"column:max_offset"`
		Count     int64  `gorm:"column:count"`
	}

	var stats []PartitionStats
	err := db.Raw(`
		SELECT 
			topic, 
			partition,
			MIN(per_partition_offset) as min_offset,
			MAX(per_partition_offset) as max_offset,
			COUNT(*) as count
		FROM mq_messages 
		GROUP BY topic, partition
		ORDER BY topic, partition
	`).Scan(&stats).Error

	if err != nil {
		return fmt.Errorf("获取统计信息失败: %w", err)
	}

	for _, stat := range stats {
		fmt.Printf("   主题: %s, 分区: %d, 偏移量范围: %d~%d, 消息数: %d\n",
			stat.Topic, stat.Partition, stat.MinOffset, stat.MaxOffset, stat.Count)
	}

	return nil
}
