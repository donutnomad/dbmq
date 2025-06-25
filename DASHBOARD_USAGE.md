# DBMQ 监控仪表板使用说明

## 概述

DBMQ 监控仪表板是一个美观的Web界面，用于实时监控和管理DBMQ消息队列系统。仪表板提供了Topic、消费组和系统状态的全面视图，支持点击查看详情功能。

## 🆕 新功能亮点

### ⚡ 高频刷新
- **实时监控**: 每2秒自动刷新数据（原来是30秒）
- **即时响应**: 快速反映系统状态变化

### 🔍 详情页面
- **Topic详情**: 点击Topic名称查看分区详情、配置信息、存储大小等
- **消费组详情**: 点击消费组查看成员信息、分区延迟、分配状态等
- **新窗口打开**: 详情页面在新标签页中打开，方便对比查看

## 功能特性

### 🎯 主要功能
- **实时监控**: 自动每2秒刷新数据
- **Topic管理**: 查看所有Topic的状态、分区数、消息数
- **消费组监控**: 监控消费组状态、成员数、延迟情况
- **系统统计**: 显示系统运行时间、总体统计信息
- **响应式设计**: 支持桌面和移动设备
- **可点击详情**: Topic和消费组支持点击查看详情

### 📊 监控指标
- Topic总数
- 消费组总数
- 总消息数
- 系统运行时间
- 分区信息
- 消费延迟

## 快速开始

### 1. 启动服务器

```bash
# 方法一：使用提供的脚本
./run_dashboard_demo.sh

# 方法二：手动构建和运行
go build -o dbmq-rest-server ./cmd/rest_api_demo/
./dbmq-rest-server
```

### 2. 访问仪表板

启动服务器后，打开浏览器访问：

- **监控仪表板**: http://localhost:8080/api/v1/dashboard
- **健康检查**: http://localhost:8080/api/v1/health
- **API根路径**: http://localhost:8080/api/v1/

## 页面说明

### 🏠 仪表板首页

仪表板分为以下几个部分：

#### 1. 系统状态头部
- 显示系统运行状态
- 最后更新时间
- 系统在线状态指示

#### 2. 统计卡片
- **Topic总数**: 当前系统中的Topic数量
- **消费组总数**: 活跃的消费组数量
- **总消息数**: 系统中的消息总数
- **系统运行时间**: 服务器运行时长

#### 3. Topic列表
显示所有Topic的详细信息：
- Topic名称 (🔗 **可点击查看详情**)
- 分区数量
- 消息数量
- 运行状态

#### 4. 消费组列表
显示所有消费组的信息：
- 消费组ID (🔗 **可点击查看详情**)
- 运行状态
- 成员数量
- 消费延迟

### 📋 Topic详情页面

点击Topic名称后，将在新窗口中打开详情页面，包含：

#### 基本信息
- 分区数量
- 消息总数
- 存储大小
- 最新偏移量

#### 🗂️ 分区详情表格
- 分区ID
- 最新偏移量
- 消息数
- 存储大小

#### ⚙️ Topic配置
- 显示所有Topic配置项
- 配置项名称和对应的值

### 👥 消费组详情页面

点击消费组ID后，将在新窗口中打开详情页面，包含：

#### 基本信息
- 运行状态（带颜色标识）
- 成员数量
- 总延迟量
- 代际ID

#### 🧑‍💼 消费者成员表格
- 消费者ID
- 客户端ID
- 主机地址
- 分配分区数
- 最后心跳时间

#### 📊 分区延迟详情
- Topic名称
- 分区ID
- 当前偏移量
- 最新偏移量
- 延迟量

#### 📋 分配的Topics
- 以标签形式显示该消费组分配的所有Topic

## API接口

### REST API端点

```
GET  /api/v1/dashboard                    # 仪表板HTML页面
GET  /api/v1/dashboard/data               # 仪表板数据API
GET  /api/v1/dashboard/topic/{topicName}  # Topic详情页面
GET  /api/v1/dashboard/consumer-group/{groupId}  # 消费组详情页面
GET  /api/v1/health                       # 健康检查
GET  /api/v1/clusters/dbmq-cluster/topics/{topicName}  # Topic详情API
GET  /api/v1/clusters/dbmq-cluster/consumer-groups/{groupId}  # 消费组详情API
```

### 使用curl测试API

```bash
# 获取仪表板数据
curl http://localhost:8080/api/v1/dashboard/data

# 获取Topic详情
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/topics/test-topic

# 获取消费组详情
curl http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups/test-group
```

## 📱 用户体验优化

### 🎨 界面设计
- **现代化UI**: 采用渐变背景和卡片式设计
- **响应式布局**: 自适应桌面和移动设备
- **颜色编码**: 不同状态使用不同颜色标识
- **加载动画**: 数据加载时显示友好的动画效果

### 🚀 性能优化
- **快速刷新**: 2秒自动刷新，及时反映系统变化
- **异步加载**: 使用异步请求避免页面阻塞
- **错误处理**: 友好的错误提示和重试机制
- **新窗口打开**: 详情页面不影响主仪表板的浏览

### 🔧 交互功能
- **可点击链接**: Topic和消费组名称可直接点击
- **悬停效果**: 表格行悬停高亮显示
- **返回按钮**: 详情页面提供便捷的返回链接
- **状态徽章**: 使用彩色徽章显示运行状态

## 兼容性说明

### 浏览器支持
- Chrome 60+
- Firefox 55+
- Safari 12+
- Edge 79+

### 移动设备
- iOS Safari 12+
- Android Chrome 60+
- 响应式设计确保良好的移动体验

## 故障排除

### 常见问题

1. **页面无法访问**
   ```bash
   # 检查服务是否启动
   curl http://localhost:8080/api/v1/health
   ```

2. **数据显示为空**
   ```bash
   # 检查是否有测试数据
   curl http://localhost:8080/api/v1/dashboard/data
   ```

3. **详情页面404错误**
   - 确保Topic或消费组名称正确
   - 检查URL编码是否正确

### 性能监控

监控仪表板本身的性能：
- 页面加载时间
- API响应时间
- 刷新频率对系统的影响

## 📈 最佳实践

1. **数据查看**：
   - 使用主仪表板进行整体监控
   - 点击详情页面深入了解特定组件
   - 利用2秒刷新及时发现问题

2. **性能考虑**：
   - 如果系统负载较高，可以考虑调整刷新频率
   - 详情页面数据量大时注意网络传输时间

3. **问题诊断**：
   - 观察消费延迟变化趋势
   - 检查Topic分区的消息分布
   - 监控消费组成员的活跃状态

## 📚 参考资料

- [Kafka UI兼容性文档](docs/kafka_ui_compatibility.md)
- [DBMQ架构设计](设计架构文档.md)
- [超级演示使用说明](超级演示使用说明.md)

## 🤝 反馈与改进

欢迎提出建议和改进意见：
- 功能增强需求
- 用户体验改进
- 性能优化建议
- 界面设计改进

## 配置说明

### 数据库配置

默认数据库配置：
```go
dsn := "root:password@tcp(localhost:3306)/dbmq_demo?charset=utf8mb4&parseTime=True&loc=Local"
```

### 服务器配置

默认服务器配置：
```go
RestAPIConfig{
    DB:     db,
    Port:   8080,
    Host:   "localhost", 
    Prefix: "/api/v1",
}
```

## 自定义配置

### 修改端口

编辑 `cmd/rest_api_demo/main.go`：
```go
apiConfig := dbmq.RestAPIConfig{
    Port: 9090, // 改为你想要的端口
    // ...其他配置
}
```

### 修改数据库连接

编辑 `initDatabase()` 函数中的DSN：
```go
dsn := "用户名:密码@tcp(主机:端口)/数据库名?charset=utf8mb4&parseTime=True&loc=Local"
```

## 开发和扩展

### 添加新的监控指标

1. 在 `metrics.go` 中定义新的指标结构
2. 在 `dashboardDataHandler` 中添加数据获取逻辑
3. 在HTML模板中添加显示组件

### 自定义样式

仪表板使用内联CSS，可以直接在 `dashboardHandler` 中修改样式。

### 添加新的API端点

在 `registerRoutes` 方法中添加新的路由：
```go
api.HandleFunc("/your-endpoint", ras.yourHandler).Methods("GET")
```

## 性能优化

- 仪表板数据会缓存30秒，避免频繁查询数据库
- 使用数据库索引优化查询性能
- 考虑使用Redis缓存热点数据

## 安全考虑

- 生产环境建议添加认证机制
- 使用HTTPS协议
- 限制访问IP范围
- 定期更新依赖包

---

## 联系和支持

如有问题或建议，请提交Issue或Pull Request。 