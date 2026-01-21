-- Migration: 添加手动分区分配配置表
-- 版本: 001
-- 日期: 2026-01-21
-- 描述: 支持通过管理平台配置手动分区分配，覆盖默认的自动分配策略

-- 创建手动分区分配配置表
CREATE TABLE IF NOT EXISTS `mq_manual_partition_assignments` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    `group_id` VARCHAR(255) NOT NULL COMMENT '消费组ID',
    `consumer_id_pattern` VARCHAR(255) NOT NULL COMMENT '消费者ID匹配模式：精确值或前缀（以*结尾表示前缀匹配）',
    `topic` VARCHAR(255) NOT NULL COMMENT 'Topic名称',
    `partition` INT UNSIGNED NOT NULL COMMENT '分区号',
    `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
    UNIQUE KEY `uk_assignment` (`group_id`, `consumer_id_pattern`, `topic`, `partition`) COMMENT '确保同一分配规则不重复',
    INDEX `idx_group` (`group_id`) COMMENT '按消费组查询索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='手动分区分配配置表，用于覆盖默认的自动分区分配策略';

-- 匹配规则说明:
-- 1. 精确匹配: consumer_id_pattern = 'worker-1' 匹配 ConsumerID = 'worker-1'
-- 2. 前缀匹配: consumer_id_pattern = 'myhost:aa:bb:cc:dd:ee:ff:*' 匹配该机器上所有自动生成的 ConsumerID
--
-- ConsumerID 格式说明:
-- - 用户指定 ClientID 时: ConsumerID = ClientID
-- - 未指定时自动生成: ConsumerID = {hostname}:{mac地址}:{uuid}
--   例如: freddeMacBook-Pro.local:aa:bb:cc:dd:ee:ff:550e8400-e29b-41d4-a716-446655440000

-- 示例数据 (可选，取消注释以插入测试数据):
-- INSERT INTO mq_manual_partition_assignments (group_id, consumer_id_pattern, topic, partition) VALUES
-- ('order-processors', 'worker-1', 'orders', 0),
-- ('order-processors', 'worker-1', 'orders', 1),
-- ('order-processors', 'myhost:aa:bb:cc:dd:ee:ff:*', 'orders', 2);
