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

**数据访问层 - DDD + 清洁架构**

项目采用 DDD (领域驱动设计) 和清洁架构，数据库访问分为两种模式：

**1. Domain Repository (领域仓储) - 单表/单领域操作**
- 位置: `internal/domain/<entity>/repo.go` (接口定义)
- 实现: `internal/repo/<entity>repo/mysql_repo.go`
- 用途: 本领域内的 CRUD 操作，不涉及跨表查询
- 特点:
  - 接口定义在 domain 层，保持领域模型的纯净
  - 使用 Mapper 进行 PO (Persistence Object) ↔ Entity 转换
  - 返回领域实体而非数据库模型
- 示例: `topic.Repo.Get()` 返回 `*topic.Topic` 领域实体

```
internal/domain/topic/repo.go     # 接口: Repo interface
internal/repo/topicrepo/
├── po.go                         # 数据库模型 (TopicPO)
├── mapper.go                     # PO ↔ Entity 转换
└── mysql_repo.go                 # MySQL 实现
```

**2. Query Layer (CQRS 查询层) - 跨表/聚合查询**
- 位置: `internal/query/`
- 用途: 复杂的跨表 JOIN 查询、统计聚合、报表数据
- 特点:
  - 直接返回 DTO (Data Transfer Object)，无需领域实体转换
  - 支持复杂 SQL、多表 JOIN、聚合函数
  - 只读操作，遵循 CQRS 读写分离原则
- 示例: `ConsumerQuery.GetConsumerGroupExtended()` 聚合多表数据

```
internal/query/
├── query.go            # 聚合入口 (Queries struct)
├── types.go            # DTO 定义
├── cluster_query.go    # 集群统计查询
├── topic_query.go      # Topic 统计查询
├── consumer_query.go   # 消费组聚合查询
└── message_query.go    # 消息搜索查询
```

**选择指南:**
| 场景 | 使用 | 原因 |
|------|------|------|
| 单实体 CRUD | Domain Repo | 保持领域模型纯净 |
| 单表简单查询 | Domain Repo | 返回领域实体 |
| 多表 JOIN | Query Layer | CQRS 读模型 |
| 统计/聚合 | Query Layer | 直接返回 DTO |
| Dashboard/报表 | Query Layer | 复杂查询优化 |

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
- 消费者初始化流程验证
- 并发消费者初始化和分区分配

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

## 消费者初始化流程

消费者从启动到能够消费消息需要经历以下步骤：

### 正常初始化流程

1. **创建阶段** (`NewConsumer`)
   - 初始化状态机 (state = Uninitialized)
   - generation_id = 0
   - 创建 Actor 和命令通道

2. **订阅阶段** (`SubscribeTopics`)
   - 状态转换: Uninitialized → Joining
   - 启动心跳循环 (每 HeartbeatInterval 发送一次)
   - 启动自动提交循环 (如果启用)

3. **首次心跳** (t = 0s)
   - Upsert(generation_id=0) 写入数据库
   - Get() 读取 generation_id=0 (协调器尚未分配)
   - 比较: 0 == 0 → 无需重新均衡

4. **协调器检测** (t ≈ RebalanceInterval)
   - 扫描活跃消费者
   - 检测到新成员
   - IncrementGenerationID (0→1)
   - UpdateAssignments 更新分配和 generation_id

5. **第二次心跳** (t = HeartbeatInterval)
   - Upsert 更新 last_heartbeat (保留 generation_id=1)
   - Get() 读取 generation_id=1
   - 比较: 1 != 0 → **触发重新均衡**
   - doRebalance:
     - Joining → Rebalancing → Ready
     - 同步 generation_id=1
     - 获取分区偏移量
   - **初始化完成** ✅

6. **就绪阶段**
   - IsReady() 返回 true
   - 可以开始 Poll() 消息

### 时间估算

- HeartbeatInterval = 3s, RebalanceInterval = 10s
- 最坏情况: ~13秒 (首次心跳 + 协调器检测 + 第二次心跳)
- 最佳情况: ~4秒 (协调器在首次心跳后立即检测)

### 调试初始化问题

如果消费者长时间停留在 Joining 状态：

1. **检查协调器是否运行**
   ```sql
   SELECT GET_LOCK('mq_coordinator_leader_lock_XXX', 0);
   ```
   如果返回 NULL，说明锁被占用 (协调器在运行)

2. **检查心跳记录**
   ```sql
   SELECT * FROM mq_consumer_heartbeats
   WHERE group_id = 'your-group';
   ```

3. **检查 generation_id**
   ```sql
   SELECT generation_id, assigned_partitions
   FROM mq_consumer_heartbeats
   WHERE group_id = 'your-group' AND consumer_id = 'your-consumer';
   ```
   应该看到 generation_id > 0 且有分区分配

4. **启用 Debug 日志**
   关键日志输出：
   - "检测到 generation 变化，触发重新均衡"
   - "✅ 消费者初始化完成"
