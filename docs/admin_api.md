# DBMQ AdminClient API 使用指南

## 概述

DBMQ的AdminClient API模仿了Apache Kafka的AdminClient设计，提供了统一、简洁的Topic管理接口。这种设计使得从Kafka迁移到DBMQ变得更加容易。

## 核心特性

- **Kafka兼容的API设计** - 与Kafka AdminClient保持一致的方法名和参数结构
- **批量操作支持** - 支持批量创建、删除Topic
- **配置管理** - 支持Topic级别的配置，如保留时间、清理策略等
- **验证模式** - 支持仅验证而不实际执行的操作
- **完整的CRUD操作** - 创建、读取、更新、删除Topic

## 快速开始

### 1. 创建AdminClient

```go
import (
    "dbmq/pkg"
    "gorm.io/gorm"
)

// 假设你已经有了数据库连接
admin, err := pkg.NewAdminClient(pkg.AdminConfig{
    DB: dbClient, // *gorm.DB
})
if err != nil {
    log.Fatal(err)
}
defer admin.Close()
```

### 2. 创建Topic

#### 基本用法
```go
req := pkg.NewTopicRequest{
    Name:          "orders",
    NumPartitions: 3,
}

err := admin.CreateTopic(context.Background(), req)
if err != nil {
    log.Printf("创建Topic失败: %v", err)
}
```

#### 带配置的Topic
```go
retentionHours := 48
req := pkg.NewTopicRequest{
    Name:          "user-events",
    NumPartitions: 5,
    Config: &pkg.TopicConfig{
        RetentionHours: &retentionHours,
        CleanupPolicy:  "delete",
    },
}

err := admin.CreateTopic(context.Background(), req)
```

#### 批量创建Topic
```go
requests := []pkg.NewTopicRequest{
    {Name: "topic1", NumPartitions: 2},
    {Name: "topic2", NumPartitions: 4},
    {Name: "topic3", NumPartitions: 1},
}

result := admin.CreateTopics(context.Background(), requests)
for _, topicResult := range result.Results {
    if topicResult.Error != nil {
        log.Printf("Topic %s 创建失败: %v", topicResult.Name, topicResult.Error)
    } else {
        log.Printf("Topic %s 创建成功", topicResult.Name)
    }
}
```

### 3. 查询Topic

#### 列出所有Topic
```go
topics, err := admin.ListTopics(context.Background())
if err != nil {
    log.Fatal(err)
}

for _, topic := range topics {
    fmt.Printf("Topic: %s\n", topic)
}
```

#### 获取Topic详细信息
```go
descriptions, err := admin.DescribeTopics(context.Background(), []string{"orders", "user-events"})
if err != nil {
    log.Fatal(err)
}

for name, desc := range descriptions {
    fmt.Printf("Topic: %s, Partitions: %d, Created: %s\n", 
        name, desc.NumPartitions, desc.CreatedAt)
}
```

### 4. 删除Topic

```go
err := admin.DeleteTopics(context.Background(), []string{"old-topic1", "old-topic2"})
if err != nil {
    log.Printf("删除Topic失败: %v", err)
}
```

## API 参考

### AdminConfig

```go
type AdminConfig struct {
    DB *gorm.DB // 数据库连接（必需）
}
```

### NewTopicRequest

```go
type NewTopicRequest struct {
    Name          string       // Topic名称（必需）
    NumPartitions int          // 分区数量（必需，必须 > 0）
    Config        *TopicConfig // Topic配置（可选）
    ValidateOnly  bool         // 是否仅验证而不实际创建
}
```

### TopicConfig

```go
type TopicConfig struct {
    RetentionMs    *int64 `json:"retention.ms,omitempty"`    // 保留时间（毫秒）
    RetentionHours *int   `json:"retention.hours,omitempty"` // 保留时间（小时）
    CleanupPolicy  string `json:"cleanup.policy,omitempty"`  // 清理策略
}
```

### TopicDescription

```go
type TopicDescription struct {
    Name          string       `json:"name"`           // Topic名称
    NumPartitions int          `json:"numPartitions"`  // 分区数量
    Config        *TopicConfig `json:"config"`         // Topic配置
    CreatedAt     time.Time    `json:"createdAt"`      // 创建时间
}
```

## 与Kafka的对比

| 操作 | Kafka AdminClient | DBMQ AdminClient |
|------|------------------|------------------|
| 创建Topic | `createTopics()` | `CreateTopics()` |
| 列出Topic | `listTopics()` | `ListTopics()` |
| 描述Topic | `describeTopics()` | `DescribeTopics()` |
| 删除Topic | `deleteTopics()` | `DeleteTopics()` |

## 最佳实践

### 1. 错误处理
```go
result := admin.CreateTopics(ctx, requests)
for _, topicResult := range result.Results {
    if topicResult.Error != nil {
        // 根据错误类型进行不同的处理
        switch err := topicResult.Error.(type) {
        case *errors.ErrTopicAlreadyExists:
            log.Printf("Topic已存在: %s", err.TopicName)
            // 可以选择忽略此错误或采取其他行动
        case *errors.ErrUnknownTopicOrPartition:
            log.Printf("Topic不存在: %s", err.Topic)
        default:
            log.Printf("未知错误: %v", err)
        }
    }
}
```

### 2. 使用验证模式
```go
// 先验证配置是否正确
req.ValidateOnly = true
result := admin.CreateTopics(ctx, []pkg.NewTopicRequest{req})
if result.Results[0].Error != nil {
    log.Printf("配置验证失败: %v", result.Results[0].Error)
    return
}

// 验证通过后再实际创建
req.ValidateOnly = false
err := admin.CreateTopic(ctx, req)
```

### 3. 合理设置分区数量
```go
// 根据预期的消费者数量和吞吐量来设置分区数
// 一般建议分区数 = 消费者数量的倍数
req := pkg.NewTopicRequest{
    Name:          "high-throughput-topic",
    NumPartitions: 12, // 可以被2、3、4、6整除
}
```

### 4. 配置保留策略
```go
// 根据业务需求设置合适的保留时间
retentionHours := 168 // 7天
req := pkg.NewTopicRequest{
    Name:          "logs",
    NumPartitions: 3,
    Config: &pkg.TopicConfig{
        RetentionHours: &retentionHours,
        CleanupPolicy:  "delete", // 或 "compact"
    },
}
```

## 迁移指南

如果你正在从Kafka迁移到DBMQ，可以按照以下步骤：

1. **替换导入** - 将Kafka AdminClient的导入替换为DBMQ的导入
2. **更新配置** - 将Kafka的Properties配置转换为DBMQ的结构体配置
3. **调整方法调用** - 方法名基本保持一致，只需调整参数格式
4. **测试验证** - 使用ValidateOnly模式测试配置的正确性

### Kafka -> DBMQ 迁移示例

**Kafka代码:**
```java
Properties props = new Properties();
props.put("retention.ms", 604800000);
props.put("cleanup.policy", "delete");

NewTopic topic = new NewTopic("orders", 3, (short) 1);
topic.configs(props);

adminClient.createTopics(Collections.singletonList(topic));
```

**DBMQ代码:**
```go
retentionMs := int64(604800000)
req := pkg.NewTopicRequest{
    Name:          "orders",
    NumPartitions: 3,
    Config: &pkg.TopicConfig{
        RetentionMs:   &retentionMs,
        CleanupPolicy: "delete",
    },
}

err := admin.CreateTopic(context.Background(), req)
```

## 常见问题

### Q: 如何处理Topic已存在的错误？
A: AdminClient会返回`ErrTopicAlreadyExists`错误类型。你可以检查错误类型来判断是否是因为Topic已存在：

```go
err := admin.CreateTopic(ctx, req)
if err != nil {
    var topicExistsErr *errors.ErrTopicAlreadyExists
    if errors.As(err, &topicExistsErr) {
        log.Printf("Topic %s 已存在，继续执行...", topicExistsErr.TopicName)
        // 可以选择忽略此错误
    } else {
        return fmt.Errorf("创建Topic失败: %w", err)
    }
}
```

### Q: 分区数量可以修改吗？
A: 不可以。和Kafka一样，Topic创建后分区数量不能修改。这是为了保证消息顺序和分区策略的一致性。

### Q: 如何设置Topic的保留时间？
A: 使用TopicConfig中的RetentionMs或RetentionHours字段。如果同时设置，RetentionMs优先级更高。

### Q: ValidateOnly模式有什么用？
A: ValidateOnly模式可以验证Topic配置的正确性而不实际创建Topic，这在自动化部署中很有用。