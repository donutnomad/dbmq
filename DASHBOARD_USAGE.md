# DBMQ 监控仪表板使用说明

## 概述

DBMQ 监控仪表板是一个美观的Web界面，用于实时监控和管理DBMQ消息队列系统。仪表板提供了Topic、消费组和系统状态的全面视图。

## 功能特性

### 🎯 主要功能
- **实时监控**: 自动每30秒刷新数据
- **Topic管理**: 查看所有Topic的状态、分区数、消息数
- **消费组监控**: 监控消费组状态、成员数、延迟情况
- **系统统计**: 显示系统运行时间、总体统计信息
- **响应式设计**: 支持桌面和移动设备

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
- Topic名称
- 分区数量
- 消息数量
- 运行状态

#### 4. 消费组列表
显示所有消费组的信息：
- 消费组ID
- 运行状态
- 成员数量
- 消费延迟

## API接口

### REST API端点

```
GET  /api/v1/dashboard          # 仪表板HTML页面
GET  /api/v1/dashboard/data     # 仪表板数据API
GET  /api/v1/health             # 健康检查
GET  /api/v1/topics             # Topic列表
GET  /api/v1/consumer-groups    # 消费组列表
```

### 数据格式

仪表板数据API返回格式：
```json
{
  "success": true,
  "data": {
    "topics": [...],
    "consumerGroups": [...],
    "system": {
      "uptime": 3600,
      "version": "1.0.0"
    },
    "timestamp": "2024-01-01T12:00:00Z"
  }
}
```

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

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查MySQL服务是否运行
   - 验证数据库连接参数
   - 确保数据库`dbmq_demo`存在

2. **端口被占用**
   - 更改配置中的端口号
   - 或者停止占用8080端口的其他服务

3. **页面无法加载**
   - 检查服务器是否正常启动
   - 确认防火墙设置
   - 查看服务器日志

### 日志查看

服务器会输出详细的访问日志：
```
[2024-01-01 12:00:00] GET /api/v1/dashboard 200 45ms
[2024-01-01 12:00:30] GET /api/v1/dashboard/data 200 12ms
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