# CLAUDE.md

这个文件为在此代码库中工作的Claude Code (claude.ai/code)提供指导。

## 开发命令

### 构建和测试
```bash
# 运行示例
make super

# 运行所有测试
go test ./...

# 运行集成测试（需要MySQL和Redis）
go test -tags=integration ./...

# 前端开发
cd dashboard-ui
npm run dev      # 开发服务器
npm run build    # 生产构建
npm run lint     # ESLint检查
```

### 数据库设置
项目根目录有`create_tables.sql`文件，包含完整的MySQL表结构定义。集成测试需要运行中的MySQL和Redis实例。

## 核心架构

### 系统概览
DBMQ是一个基于MySQL和Redis的消息队列系统，旨在与Apache Kafka保持API兼容性。它使用MySQL作为消息持久化存储，Redis提供可选的实时通知优化。

### 关键组件

**Producer (`producer.go`)**
- 负责消息发送到指定Topic/分区
- 使用哈希或轮询方式进行分区路由
- 可选地通过Redis发送实时通知（智能通知合并机制，避免惊群效应）

**Consumer (`consumer.go`)**
- 支持消费组协议和自动重新均衡
- 实现心跳机制和偏移量管理
- 支持自动和手动提交模式
- 包含Redis实时通知订阅（可选）

**Coordinator (`coordinator.go`)**
- 管理消费组成员和分区分配
- 通过领导者选举确保高可用性
- 负责重新均衡触发和消息清理任务
- 使用MySQL全局锁实现领导者选举

**数据访问层 (`internal/dao/`)**
- `dao.go`: 统一的数据访问接口
- `message.go`: 消息持久化操作
- `topic.go`: Topic元数据管理
- `consumer.go`: 消费者状态和偏移量管理

### 数据模型核心概念

**Offset分区隔离**
- 每个分区维护独立的`per_partition_offset`字段，从0开始计数
- 避免了早期版本中不同Topic间offset混淆的严重问题
- 确保消费者可以正确从分区开头消费

**Generation ID机制**
- 消费组重新均衡时会递增`generation_id`
- 防止旧代消费者提交无效偏移量
- 实现分布式环境下的状态隔离

**双重偏移量状态**
- `committed_offset`: 数据库中已确认的最后消息偏移量
- `polledOffsets`: 内存中已拉取但未提交的最大偏移量

### REST API (`rest_api.go`)
提供HTTP接口用于监控和管理，支持Dashboard UI的数据获取。

### Dashboard UI (`dashboard-ui/`)
基于Next.js的现代化Web管理界面：
- 实时监控仪表板
- Topic和消费组管理
- 响应式设计，支持桌面和移动设备

## 重要实现细节

### 智能通知合并
Producer使用Redis Lua脚本实现"从静默到活跃"的一次性通知，避免高吞吐场景下的惊群效应。

### 重新均衡安全协议
1. 协调器递增`generation_id`隔离旧代消费者
2. 消费者检测到变化后暂停处理
3. 安全提交被撤销分区的偏移量
4. 获取新分配并恢复处理

### 消息清理机制
- 双重条件：消费低水位线 + 时间保留策略
- 批量删除避免长时间表锁
- 仅在Leader Coordinator中执行

### 心跳记录清理机制
- 定期清理过期的`mq_consumer_heartbeats`记录
- 删除条件：`last_heartbeat < (当前时间 - HeartbeatRetentionAge)`
- 分页删除避免锁表，每批删除500条记录
- 集成到消息清理循环中，使用相同的清理间隔

### 提交模式差异
- **自动提交模式**: Close()时自动提交，重新均衡时自动提交被撤销分区
- **手动提交模式**: Close()时不提交，重新均衡时不提交，记录警告日志

## 测试策略

### 单元测试
使用go-sqlmock进行数据库操作测试，专注于单个组件的逻辑正确性。

### 集成测试
需要真实的MySQL和Redis实例，测试完整的生产-消费流程。使用`//go:build integration`标签。

### 关键测试场景
- 多Topic、多分区offset隔离
- 消费组重新均衡流程
- 手动vs自动提交模式差异
- 协调器领导者选举和故障转移

## 配置管理

### 生产者配置
- `NotificationEnabled`: 启用Redis优化
- `DB`: 数据库连接
- `Redis`: Redis连接（可选）

### 消费者配置  
- `GroupID`: 消费组标识
- `EnableAutoCommit`: 提交模式选择
- `ConsumeStrategy`: earliest/latest消费策略
- `PollFetchLimit`: 批量大小

### 协调器配置  
- `HeartbeatTimeout`: 心跳超时时间
- `RebalanceInterval`: 重新均衡检查间隔
- `RetentionCheckInterval`: 消息清理间隔
- `HeartbeatRetentionAge`: 心跳记录保留期（默认7天）

## 部署注意事项

### MySQL要求
- 版本 >= 5.7
- 需要调优`innodb_buffer_pool_size`等参数
- 建议启用慢查询日志监控

### Redis配置（可选）
- 支持单机、Sentinel或Cluster模式
- 用于性能优化，短暂不可用不影响系统正确性

### 协调器部署
- 应部署多个实例形成高可用集群
- 通过MySQL全局锁自动选举Leader
- 轻量级，主要消耗CPU和数据库连接

## 监控指标

- **消费者延迟**: `(MAX(per_partition_offset) - committed_offset)` 每分区
- **消息吞吐率**: `mq_messages`表INSERT速率  
- **重新均衡频率**: `mq_consumer_group_generations`表UPDATE频率