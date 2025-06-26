# DBMQ Dashboard UI 集成说明

## 项目概述

这是一个基于 Next.js + Tailwind CSS 构建的 DBMQ 监控仪表板界面，用于替换原有的静态 HTML/CSS/JS 文件。通过使用现代化的前端技术栈，我们获得了更好的开发体验、组件化架构和类型安全。

## 与现有系统的集成

### 原有架构
```
templates/static/
├── css/                 # 样式文件
├── js/                  # JavaScript 逻辑
├── dashboard.html       # 仪表板页面
├── topic_detail.html    # Topic详情页面
├── create_topic.html    # 创建Topic页面
└── ...                  # 其他页面
```

### 新架构
```
dashboard-ui/
├── components/          # React 组件
├── lib/                # API客户端和工具
├── app/                # Next.js 页面
└── out/                # 构建生成的静态文件
```

## 部署方式

### 方式1：替换静态文件（推荐）
1. 构建项目：`npm run build`
2. 将 `out/` 目录中的文件复制到 `templates/static/`
3. 更新 Go 后端的路由配置

### 方式2：独立部署
1. 构建项目：`npm run build`
2. 将 `out/` 目录部署到独立的 Web 服务器
3. 配置 CORS 允许跨域访问

## 后端集成更改

### 1. 更新路由处理

需要在 `rest_api.go` 中更新静态文件处理：

```go
// 在 registerRoutes 方法中添加
func (ras *RestAPIServer) registerRoutes() {
    // ... 现有代码 ...
    
    // 服务静态文件
    ras.engine.Static("/static", "./templates/static")
    
    // 默认路由重定向到dashboard
    ras.engine.GET("/", func(c *gin.Context) {
        c.Redirect(http.StatusMovedPermanently, "/static/")
    })
}
```

### 2. API 端点兼容性

新的前端完全兼容现有的 API 端点：

- ✅ `/api/v1/dashboard/data` - 仪表板数据
- ✅ `/api/v1/clusters/:clusterId/topics` - Topic 管理
- ✅ `/api/v1/clusters/:clusterId/consumer-groups` - 消费组管理
- ✅ `/api/v1/health` - 健康检查

### 3. 响应格式

API 响应格式保持不变，新前端已适配：

```json
{
  "success": true,
  "data": { ... },
  "error": "...",
  "message": "..."
}
```

## 功能对比

| 功能 | 原版本 | 新版本 | 改进点 |
|------|--------|--------|--------|
| 仪表板 | ✅ | ✅ | 更好的响应式设计、组件化 |
| Topic创建 | ✅ | ✅ | 表单验证、错误处理 |
| Topic详情 | ✅ | 🚧 | 计划中 |
| 消费组详情 | ✅ | 🚧 | 计划中 |
| 消息生产 | ✅ | 🚧 | 计划中 |
| 主题管理 | ✅ | 🚧 | 计划中 |

## 开发优势

### 类型安全
- TypeScript 提供编译时类型检查
- 减少运行时错误
- 更好的 IDE 支持

### 组件化
- 可复用的 UI 组件
- 更易维护的代码结构
- 统一的设计系统

### 现代化工具链
- Hot Reload 快速开发
- 自动代码优化
- Tree Shaking 减少包大小

### 开发体验
- ESLint + Prettier 代码质量
- 自动化构建流程
- 丰富的 React 生态系统

## 迁移计划

### 阶段 1：核心功能（已完成）
- [x] 项目初始化
- [x] 基础 UI 组件
- [x] API 客户端
- [x] 主仪表板
- [x] Topic 创建页面

### 阶段 2：完整功能迁移
- [ ] Topic 详情页面
- [ ] 消费组详情页面
- [ ] 消息生产界面
- [ ] Topic 管理页面

### 阶段 3：功能增强
- [ ] 实时图表
- [ ] 高级过滤和搜索
- [ ] 批量操作
- [ ] 配置管理

## 使用指南

### 开发环境
```bash
cd dashboard-ui
npm install
npm run dev
```
访问：http://localhost:3000

### 生产构建
```bash
npm run build
./deploy.sh ../templates/static-new
```

### 环境变量
```bash
# .env.local
NEXT_PUBLIC_API_BASE_URL=http://localhost:8080
NEXT_PUBLIC_API_PREFIX=/api/v1
```

## 注意事项

1. **API 代理**：开发环境使用 Next.js rewrites 代理 API 请求
2. **静态导出**：生产环境导出为纯静态文件，无需 Node.js 运行时
3. **浏览器兼容性**：支持现代浏览器，IE 需要 polyfill
4. **CORS**：如果独立部署，需要配置后端 CORS

## 疑难解答

### 构建失败
- 检查 Node.js 版本（需要 18+）
- 确保所有依赖正确安装
- 查看 ESLint 错误并修复

### API 请求失败
- 确认后端服务运行在正确端口
- 检查 CORS 配置
- 验证 API 端点路径

### 样式问题
- 确认 Tailwind CSS 配置正确
- 检查 CSS 类名拼写
- 验证响应式断点 