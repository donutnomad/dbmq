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

### 代码生成

修改 API 定义后需要重新生成代码：

```bash
# 重新生成 API 路由代码（修改 dbmqapi/*.go 中的 @GET/@POST 注解后）
cd dbmqapi
go tool gogen ./...

# 注意：
# 1. ❌ 不要使用 @PREFIX + @GET(/) 的组合，会生成带尾部斜杠的路由
#    例如：@PREFIX(/api/v1/topics) + @GET(/) → /api/v1/topics/ (多了斜杠!)
#
# 2. ✅ 直接在 @GET 中写完整路径
#    例如：@GET(/api/v1/consumer-groups) → /api/v1/consumer-groups (正确!)
#
# 3. 修改后必须运行 go tool gogen 重新生成 generate.go 文件
#
# 4. 生成后检查 generate.go 中的路由定义，确保没有尾部斜杠
```

**正确示例**：

```go
// ConsumerGroupAPI 消费组管理 API
// @TAG(Consumer-Group)
type ConsumerGroupAPI interface {
    // List 获取消费组列表
    // @GET(/api/v1/consumer-groups)  ← 完整路径，无尾部斜杠
    List(ctx context.Context) ([]ConsumerGroupResp, error)
}
```

**错误示例**：

```go
// ❌ 不要这样写
// @PREFIX(/api/v1/consumer-groups)
type ConsumerGroupAPI interface {
    // @GET(/)  ← 会生成 /api/v1/consumer-groups/ (多了斜杠!)
    List(ctx context.Context) ([]ConsumerGroupResp, error)
}
```

### 测试脚本

```bash
# 测试 API 路由是否正确（不带尾部斜杠）
./test-routes.sh

# 测试 CORS 配置
./test-cors.sh

# 重启演示服务
./restart-demo.sh
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

**前端 API 配置**：
- 配置文件位置：`dashboard-ui/config/api.config.ts`（唯一配置源）
- 环境变量：创建 `dashboard-ui/.env.local` 设置 `NEXT_PUBLIC_API_BASE_URL`
- 默认后端地址：`http://localhost:8081`
- 详细说明：参考 `dashboard-ui/config/README.md`

## 重要实现细节

### CORS 跨域配置

后端服务器（`dbmqapi/server.go`）已配置 CORS 中间件：

```go
func corsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        origin := c.Request.Header.Get("Origin")
        if origin == "" {
            origin = "*"
        }

        c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
        c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
        c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
        c.Writer.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token, X-Requested-With")
        c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers")
        c.Writer.Header().Set("Access-Control-Max-Age", "86400")

        if c.Request.Method == "OPTIONS" {
            c.AbortWithStatus(http.StatusNoContent)
            return
        }

        c.Next()
    }
}
```

**注意**：
- 开发环境允许所有源（动态设置 Origin）
- 生产环境应添加源白名单验证
- OPTIONS 预检请求返回 204 No Content
- 预检结果缓存 24 小时（减少预检请求频率）

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

## 常见问题和故障排查

### API 路由问题

**问题**：前端请求 `/api/v1/consumer-groups` 返回 301 重定向

**原因**：路由定义中包含尾部斜杠（`/api/v1/consumer-groups/`）

**解决方案**：
1. 修改 `dbmqapi/api_*.go` 文件中的路由定义
2. 移除 `@PREFIX` 注解，在 `@GET/@POST` 中写完整路径
3. 运行 `cd dbmqapi && go tool gogen ./...` 重新生成代码
4. 重启服务：`./restart-demo.sh`

### CORS 跨域错误

**问题**：浏览器控制台显示 CORS 错误

**检查步骤**：
1. 确认后端服务运行在 `http://localhost:8081`
2. 检查 `dbmqapi/server.go` 中的 CORS 中间件是否正确配置
3. 清除浏览器缓存（预检请求可能被缓存）
4. 使用 `./test-cors.sh` 测试 CORS 配置

**常见原因**：
- 后端服务未启动或端口不正确
- CORS 中间件未注册到 Gin 引擎
- OPTIONS 预检请求返回错误状态码
- **浏览器缓存了旧的预检请求结果**（最常见）

**浏览器缓存问题** ⚠️：

CORS 预检请求（OPTIONS）会被浏览器缓存，缓存时间由 `Access-Control-Max-Age` 控制：
- 开发环境设置为 **600 秒（10 分钟）**
- 生产环境可以设置为 **86400 秒（24 小时）**

如果修改了后端 CORS 配置或路由，但前端仍报错，可能是缓存问题：

**解决方法**：

1. **清空缓存并硬性重新加载**（推荐）
   - Chrome/Edge：开发者工具（F12）→ 右键点击刷新按钮 → "清空缓存并硬性重新加载"

2. **使用无痕模式**
   - Chrome：Ctrl+Shift+N (Windows) 或 Cmd+Shift+N (Mac)
   - 无痕模式不使用缓存，可以快速验证是否是缓存问题

3. **开发时禁用缓存**
   - 开发者工具（F12）→ Network 标签 → 勾选 "Disable cache"
   - 保持开发者工具打开状态

4. **临时禁用预检缓存**（调试用）
   - 修改 `dbmqapi/server.go` 中的 `Access-Control-Max-Age` 为 `"0"`
   - 重启后端服务
   - 调试完成后改回 `"600"` 或 `"86400"`

### 前端无法加载数据

**问题**：前端页面显示"加载失败"或数据为空

**排查步骤**：

1. **检查后端服务**：
   ```bash
   curl http://localhost:8081/api/v1/health
   ```
   应返回 200 状态码

2. **检查 API 配置**：
   - 查看 `dashboard-ui/config/api.config.ts`
   - 确认 `API_BASE_URL` 是否正确
   - 开发模式下检查页面底部的调试信息面板

3. **检查网络请求**：
   - 打开浏览器开发者工具 → Network 标签
   - 查看请求 URL 是否正确
   - 查看响应状态码和数据

4. **检查环境变量**：
   ```bash
   # 查看前端配置
   cat dashboard-ui/.env.local

   # 应该包含
   NEXT_PUBLIC_API_BASE_URL=http://localhost:8081
   ```

5. **重启服务**：
   ```bash
   # 停止旧进程
   pkill -f integration_demo

   # 重启后端
   ./restart-demo.sh

   # 重启前端（新终端）
   cd dashboard-ui && npm run dev
   ```

### 路由 301 重定向循环

**问题**：请求不断重定向，无法获取数据

**原因**：Gin 默认的 `RedirectTrailingSlash` 配置

**解决方案**：
- 不要依赖自动重定向
- 修改 API 定义，确保路由不包含尾部斜杠
- 重新生成代码：`cd dbmqapi && go tool gogen ./...`

### 代码生成失败

**问题**：运行 `go tool gogen` 报错

**检查步骤**：
1. 确认 gogen 工具已安装：
   ```bash
   go tool gogen -h
   ```

2. 检查 API 定义语法是否正确：
   - `@GET(...)` 路径必须以 `/` 开头
   - 参数占位符使用 `{paramName}` 格式
   - 注释格式必须严格遵守

3. 查看错误信息，定位到具体文件和行号

### 生产环境部署注意事项

**CORS 安全配置**：

生产环境不应允许所有源，需要添加白名单：

```go
// dbmqapi/server.go
func corsMiddleware() gin.HandlerFunc {
    return func(c *gin.Context) {
        origin := c.Request.Header.Get("Origin")

        // 生产环境白名单
        allowedOrigins := []string{
            "https://your-frontend.com",
            "https://www.your-frontend.com",
        }

        allowed := false
        for _, allowed := range allowedOrigins {
            if origin == allowedOrigin {
                allowed = true
                break
            }
        }

        if allowed {
            c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
            // ... 其他 CORS 头
        } else {
            c.AbortWithStatus(http.StatusForbidden)
            return
        }

        // ... 其他逻辑
    }
}
```

**环境变量配置**：

```bash
# 生产环境前端配置
NEXT_PUBLIC_API_BASE_URL=https://api.your-domain.com

# 生产环境后端配置
MYSQL_DSN=user:pass@tcp(mysql-host:3306)/dbmq_prod?charset=utf8mb4
REDIS_ADDR=redis-host:6379
REDIS_PASSWORD=your-redis-password
```

