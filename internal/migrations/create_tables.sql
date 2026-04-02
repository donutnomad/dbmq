-- 创建dbmq消息队列系统的数据库表
-- 基于 /internal/db/schema.go 生成

-- Topic元数据表
CREATE TABLE `mq_topics` (
    `topic_name` VARCHAR(255) NOT NULL COMMENT 'Topic名称',
    `partition_count` INT UNSIGNED NOT NULL COMMENT '分区数量，创建后不可修改',
    `configs` JSON NOT NULL COMMENT 'Topic级别配置, e.g. {"retention_ms": 604800000}',
    `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (`topic_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Topic元数据表';

-- 消息持久化日志表
CREATE TABLE `mq_messages` (
    `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '全局唯一ID，用于消息排序和消费',
    `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '消息创建时间',
    `topic` VARCHAR(255) NOT NULL COMMENT '消息所属Topic',
    `partition` INT UNSIGNED NOT NULL COMMENT '消息所属分区',
    `message_key` VARCHAR(255) NOT NULL COMMENT '消息的业务Key, 用于分区策略',
    `headers` JSON NOT NULL COMMENT '消息头信息',
    `body` JSON NOT NULL COMMENT '消息体',
    PRIMARY KEY (`id`),
    INDEX `idx_created_at` (`created_at`),
    INDEX `idx_message_key` (`message_key`),
    INDEX `idx_consume_pull` (`topic`, `partition`, `id`),
    INDEX `idx_topic_partition_created` (`topic`, `partition`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息持久化日志表';

-- 消费组代际与元数据表
CREATE TABLE `mq_consumer_group_generations` (
    `group_id` VARCHAR(255) NOT NULL COMMENT '消费组ID',
    `generation_id` INT UNSIGNED NOT NULL COMMENT '代际ID, 每次再均衡时加一',
    `protocol_type` VARCHAR(50) NOT NULL DEFAULT 'consumer' COMMENT '协议类型',
    `leader_id` VARCHAR(255) NOT NULL DEFAULT '' COMMENT '消费组领导者ID',
    `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
    PRIMARY KEY (`group_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消费组代际与元数据表';

-- 消费者心跳与分区分配表
CREATE TABLE `mq_consumer_heartbeats` (
    `group_id` VARCHAR(255) NOT NULL COMMENT '消费组ID',
    `consumer_id` VARCHAR(255) NOT NULL COMMENT '消费者唯一ID',
    `generation_id` INT UNSIGNED NOT NULL COMMENT '消费者当前所属的代际ID',
    `subscribed_topics` JSON NOT NULL COMMENT '订阅的Topic列表',
    `assigned_partitions` JSON NOT NULL COMMENT '被分配的分区',
    `offline` BOOLEAN NOT NULL DEFAULT FALSE COMMENT '是否已下线：true=主动下线，false=在线或超时',
    `last_heartbeat` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '最后心跳时间',
    `offline_at` TIMESTAMP(3) NULL COMMENT '下线时间，仅当offline=true时有效',
    PRIMARY KEY (`group_id`, `consumer_id`),
    INDEX `idx_last_heartbeat` (`last_heartbeat`),
    INDEX `idx_offline_status` (`offline`, `last_heartbeat`),
    INDEX `idx_generation_id` (`generation_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消费者心跳与分区分配表';

-- 消费组消费进度跟踪表
CREATE TABLE `mq_consumer_group_consumption_progress` (
    `group_id` VARCHAR(255) NOT NULL COMMENT '消费组ID',
    `topic` VARCHAR(255) NOT NULL COMMENT 'Topic名称',
    `partition` INT UNSIGNED NOT NULL COMMENT '分区号',
    `last_consumed_message_id` BIGINT NOT NULL DEFAULT -1 COMMENT '最后成功消费的消息ID，-1表示还未消费任何消息',
    `subscription_registered_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '消费组首次订阅此分区的时间',
    `subscription_start_watermark` BIGINT NULL COMMENT '订阅时topic的最新消息ID，用于区分消费策略',
    `generation_id` INT UNSIGNED NOT NULL COMMENT '最后更新此记录时的代际ID，用于并发控制',
    `metadata` VARCHAR(255) DEFAULT '' COMMENT '可选的元数据信息',
    `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
    PRIMARY KEY (`group_id`, `topic`, `partition`),
    INDEX `idx_topic_partition` (`topic`, `partition`),
    INDEX `idx_generation_id` (`generation_id`),
    INDEX `idx_last_consumed` (`last_consumed_message_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消费组消费进度跟踪表';

-- 手动分区分配配置表
-- 用于覆盖默认的自动分区分配策略，支持将特定分区固定分配给特定消费者
-- 匹配规则说明:
--   - 精确匹配: consumer_id_pattern = "worker-1" 匹配 ConsumerID = "worker-1"
--   - 前缀匹配: consumer_id_pattern = "myhost:aa:bb:cc:dd:ee:ff:*" 匹配该前缀开头的所有 ConsumerID
CREATE TABLE `mq_manual_partition_assignments` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    `group_id` VARCHAR(255) NOT NULL COMMENT '消费组ID',
    `consumer_id_pattern` VARCHAR(255) NOT NULL COMMENT '消费者ID匹配模式：精确值或前缀（以*结尾表示前缀匹配）',
    `topic` VARCHAR(255) NOT NULL COMMENT 'Topic名称',
    `partition` INT UNSIGNED NOT NULL COMMENT '分区号',
    `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '最后更新时间',
    UNIQUE KEY `uk_assignment` (`group_id`, `consumer_id_pattern`, `topic`, `partition`) COMMENT '确保同一分配规则不重复',
    INDEX `idx_group` (`group_id`) COMMENT '按消费组查询索引'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='手动分区分配配置表';

-- 协调器 Leader 选举锁表（基于 CAS 方式，由 dbleader 库管理）
CREATE TABLE IF NOT EXISTS `mq_coordinator_leader_lock` (
    `lock_name`   VARCHAR(64)     NOT NULL COMMENT '锁名称/组件名',
    `leader_ip`   VARCHAR(64)     NOT NULL COMMENT '当前持有锁的节点地址',
    `expire_time` DATETIME(3)     NOT NULL COMMENT '锁的绝对过期时间',
    `version`     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '每次易主 +1，Fencing Token',
    PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='协调器Leader选举锁表';