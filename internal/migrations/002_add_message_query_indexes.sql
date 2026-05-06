-- Migration: 为消息列表和 Topic 维度查询补充复合索引
-- 版本: 002
-- 日期: 2026-05-06
-- 描述: 优化 dashboard、consumer-groups、topic messages 的消息表访问路径

ALTER TABLE `mq_messages`
    ADD INDEX `idx_topic_created_id` (`topic`, `created_at` DESC, `id` DESC),
    ADD INDEX `idx_topic_partition_created_id` (`topic`, `partition`, `created_at` DESC, `id` DESC);
