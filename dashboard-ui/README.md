# DBMQ Dashboard UI

基于 Next.js + Tailwind CSS 构建的 DBMQ 消息队列监控仪表板界面。

## 功能特性

- 📊 **实时监控仪表板** - 显示系统状态、Topic 和消费组统计
- 🔄 **自动刷新** - 支持自动刷新数据，实时监控系统状态
- 📝 **Topic 管理** - 创建、查看和管理 Topic
- 👥 **消费组监控** - 查看消费组状态和延迟信息
- 📱 **响应式设计** - 适配桌面和移动设备
- 🎨 **现代化 UI** - 使用 Tailwind CSS 构建的美观界面

## 技术栈

- **Next.js 15** - React 全栈框架
- **TypeScript** - 类型安全的 JavaScript
- **Tailwind CSS** - 实用优先的 CSS 框架
- **Lucide React** - 现代化图标库
- **Axios** - HTTP 客户端

## 开始使用

### 前置要求

- Node.js 18+ 
- npm 或 yarn
- DBMQ 后端服务运行在 localhost:8080

### 安装依赖

\`\`\`bash
npm install
\`\`\`

### 开发环境

\`\`\`bash
npm run dev
\`\`\`

访问 [http://localhost:3000](http://localhost:3000) 查看应用。

### 生产构建

\`\`\`bash
npm run build
\`\`\`

构建的静态文件将生成在 \`out\` 目录中，可以直接部署到任何静态文件服务器。

## 项目结构

\`\`\`
dashboard-ui/
├── app/                    # Next.js App Router 页面
│   ├── layout.tsx         # 根布局
│   ├── page.tsx          # 主页（仪表板）
│   └── topics/           # Topic 相关页面
├── components/            # React 组件
│   ├── ui/               # 基础 UI 组件
│   └── dashboard.tsx     # 主仪表板组件
├── lib/                  # 工具和 API 客户端
│   ├── api.ts           # DBMQ API 客户端
│   ├── types.ts         # TypeScript 类型定义
│   └── utils.ts         # 工具函数
├── public/              # 静态资源
└── package.json         # 项目配置
\`\`\`

## API 配置

项目通过以下方式连接到 DBMQ 后端：

### 开发环境
- 使用 Next.js rewrites 代理 API 请求到 \`http://localhost:8080\`
- 配置在 \`next.config.ts\` 中

### 生产环境
- 静态文件直接请求后端 API
- 通过环境变量 \`NEXT_PUBLIC_API_BASE_URL\` 配置后端地址

## 环境变量

创建 \`.env.local\` 文件：

\`\`\`
NEXT_PUBLIC_API_BASE_URL=http://localhost:8080
NEXT_PUBLIC_API_PREFIX=/api/v1
\`\`\`

## 页面说明

### 主仪表板 (\`/\`)
- 系统状态概览
- Topic 和消费组列表
- 实时统计数据
- 自动刷新功能

### Topic 创建 (\`/topics/create\`)
- Topic 配置表单
- 分区数和副本因子设置
- 实时创建反馈

### Topic 详情 (\`/topics/[name]\`)
- Topic 详细信息
- 分区状态
- 消息统计

### 消费组详情 (\`/consumer-groups/[groupId]\`)
- 消费组状态
- 成员信息
- 延迟统计

## 部署说明

### 静态部署
1. 运行 \`npm run build\` 构建项目
2. 将 \`out\` 目录中的文件部署到静态文件服务器
3. 配置服务器处理 SPA 路由（如 nginx try_files）

### 与 DBMQ 后端集成
项目设计为与 DBMQ 后端无缝集成：

1. 开发时通过代理访问后端 API
2. 生产时作为静态文件部署，直接请求后端
3. 支持通过环境变量配置后端地址

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
