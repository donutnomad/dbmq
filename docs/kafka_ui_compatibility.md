# DBMQ Kafka UI 兼容性指南

## 概述

DBMQ现在提供了完整的REST API接口，兼容主流的Kafka UI管理工具，包括：
- **Kafka UI (Provectus/Kafbat)**
- **Conduktor**
- **Redpanda Console**
- **Lenses.io**

## 🚀 快速开始

### 1. 启动DBMQ监控服务

```bash
# 编译并运行监控演示程序
cd cmd/metrics_demo
go run main.go
```

### 2. 访问REST API接口

服务启动后，可通过以下端点访问监控数据：

#### 🏥 健康检查
```bash
curl http://localhost:8080/api/v1/health
curl http://localhost:8080/api/v1/actuator/health  # Spring Boot风格
```

#### 🏢 集群信息
```bash
# 集群列表
curl http://localhost:8080/api/v1/clusters

# 集群指标
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/metrics

# Broker信息
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/brokers
```

#### 📁 Topic管理
```bash
# Topic列表
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/topics
curl http://localhost:8080/api/v1/topics  # Kafka REST Proxy风格

# 单个Topic详情
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/topics/{topicName}

# Topic指标
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/topics/{topicName}/metrics

# 创建Topic
curl -X POST http://localhost:8080/api/v1/clusters/dbmq-cluster/topics \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-topic",
    "numPartitions": 3,
    "config": {
      "retention.hours": 24
    }
  }'
```

#### 👥 消费组管理
```bash
# 消费组列表
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups

# 单个消费组详情
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups/{groupId}

# 消费组指标
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups/{groupId}/metrics
```

#### 📊 DBMQ专用统计
```bash
# 完整统计信息
curl http://localhost:8080/api/v1/dbmq/stats

# Topic消息浏览
curl "http://localhost:8080/api/v1/dbmq/topics/{topicName}/messages?partition=0&limit=10"
```

## 🔧 兼容Kafka UI工具配置

### Kafka UI (Kafbat/Provectus)

#### 方式1: 直接使用DBMQ REST API
```yaml
# docker-compose.yml
version: '3.8'
services:
  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: "DBMQ Cluster"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "dbmq-api:8080"
      KAFKA_CLUSTERS_0_PROPERTIES_SECURITY_PROTOCOL: "PLAINTEXT"
      # 使用REST API模式
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_NAME: "DBMQ REST API"
      KAFKA_CLUSTERS_0_KAFKACONNECT_0_ADDRESS: "http://dbmq-api:8080/api/v1"
    depends_on:
      - dbmq-api

  dbmq-api:
    build: .
    command: ["./metrics_demo"]
    ports:
      - "8080:8080"
    environment:
      DB_HOST: "mysql"
      DB_PORT: "3306"
      DB_NAME: "dbmq"
      DB_USER: "root"
      DB_PASSWORD: "password"
```

#### 方式2: 使用适配器模式
```yaml
# 创建Kafka协议适配器
services:
  dbmq-kafka-adapter:
    image: dbmq-kafka-adapter:latest  # 需要额外开发
    ports:
      - "9092:9092"
    environment:
      DBMQ_API_URL: "http://dbmq-api:8080/api/v1"
      KAFKA_PORT: "9092"
```

### Conduktor配置

```yaml
# conduktor-config.yml
version: '3'
clusters:
  - name: "DBMQ Cluster"
    bootstrapServers: "localhost:8080"  # 指向DBMQ REST API
    properties:
      security.protocol: "PLAINTEXT"
    # 使用REST API模式
    restProxy:
      url: "http://localhost:8080/api/v1"
      enabled: true
```

### Redpanda Console配置

```yaml
# redpanda-console-config.yml
kafka:
  brokers: ["localhost:8080"]  # 指向DBMQ REST API
  
rest:
  enabled: true
  urls: ["http://localhost:8080/api/v1"]

console:
  enabled: true
```

## 📈 监控指标说明

### 集群级别指标
- `clusterId`: 集群标识符
- `brokerCount`: Broker数量（DBMQ固定为1）
- `topicCount`: Topic总数
- `partitionCount`: 分区总数
- `messageCount`: 消息总数
- `consumerGroups`: 消费组数量
- `activeConsumers`: 活跃消费者数量

### Topic级别指标
- `topicName`: Topic名称
- `partitionCount`: 分区数量
- `messageCount`: 消息总数
- `latestOffset`: 最新偏移量
- `sizeBytes`: 存储大小
- `partitions[]`: 分区详细信息
- `config{}`: Topic配置

### 消费组级别指标
- `groupId`: 消费组ID
- `state`: 状态（Active/Dead）
- `members[]`: 成员信息
- `lag`: 总延迟
- `partitionLags[]`: 分区延迟详情
- `generationId`: 代际ID

### Broker级别指标
- `brokerId`: Broker ID
- `host`: 主机地址
- `port`: 端口号
- `isController`: 是否为控制器
- `uptime`: 运行时间
- `version`: 版本信息

## 🔌 扩展开发

### 自定义指标
```go
// 扩展MetricsClient添加自定义指标
func (mc *MetricsClient) GetCustomMetrics(ctx context.Context) (*CustomMetrics, error) {
    // 实现自定义指标逻辑
}
```

### 添加新的API端点
```go
// 在rest_api.go中添加新的处理器
func (ras *RestAPIServer) customHandler(w http.ResponseWriter, r *http.Request) {
    // 实现自定义API逻辑
}

// 在registerRoutes中注册
api.HandleFunc("/custom/endpoint", ras.customHandler).Methods("GET")
```

### JMX兼容性
```go
// 可以添加JMX支持以兼容更多工具
type JMXExporter struct {
    metricsClient *MetricsClient
}

func (jmx *JMXExporter) ExportMetrics() {
    // 导出JMX格式的指标
}
```

## 🛠️ 故障排除

### 常见问题

1. **API无法访问**
   ```bash
   # 检查服务是否启动
   curl http://localhost:8080/api/v1/health
   
   # 检查端口是否被占用
   lsof -i :8080
   ```

2. **数据库连接失败**
   ```bash
   # 检查数据库配置
   echo $DB_HOST $DB_PORT $DB_NAME
   
   # 测试数据库连接
   mysql -h $DB_HOST -P $DB_PORT -u $DB_USER -p$DB_PASSWORD $DB_NAME
   ```

3. **指标数据为空**
   ```bash
   # 检查是否有数据
   curl http://localhost:8080/api/v1/dbmq/stats
   
   # 创建测试数据
   curl -X POST http://localhost:8080/api/v1/clusters/dbmq-cluster/topics \
     -H "Content-Type: application/json" \
     -d '{"name": "test-topic", "numPartitions": 1}'
   ```

### 性能优化

1. **缓存指标数据**
   - 对于频繁访问的指标，可以添加Redis缓存
   - 设置合理的缓存过期时间（如30秒）

2. **数据库索引优化**
   ```sql
   -- 为监控查询添加索引
   CREATE INDEX idx_messages_topic_partition ON mq_messages(topic, partition);
   CREATE INDEX idx_heartbeat_group_time ON mq_consumer_heartbeats(group_id, last_heartbeat);
   ```

3. **分页查询**
   ```go
   // 对大数据量查询使用分页
   func (mc *MetricsClient) GetTopicsWithPaging(ctx context.Context, offset, limit int) ([]TopicMetrics, error) {
       // 实现分页逻辑
   }
   ```

## 📚 参考资料

- [Kafka REST Proxy API文档](https://docs.confluent.io/platform/current/kafka-rest/api.html)
- [Kafka UI项目](https://github.com/kafbat/kafka-ui)
- [Conduktor文档](https://docs.conduktor.io/)
- [Redpanda Console文档](https://docs.redpanda.com/docs/console/)
- [Spring Boot Actuator](https://docs.spring.io/spring-boot/docs/current/reference/html/actuator.html)

## 🤝 贡献

欢迎提交Issue和Pull Request来改进DBMQ的监控功能和Kafka UI兼容性！