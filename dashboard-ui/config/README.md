# API 配置说明

## 配置文件位置

**所有 API 配置统一在以下文件管理：**

```
dashboard-ui/config/api.config.ts  ← 唯一的配置源
```

## 环境变量

创建 `.env.local` 文件来覆盖默认配置：

```bash
cd dashboard-ui
cat > .env.local << 'EOF'
# DBMQ 后端 API 地址
NEXT_PUBLIC_API_BASE_URL=http://localhost:8081
EOF
```

**注意事项：**
- `.env.local` 文件不会被 git 追踪（已在 .gitignore 中）
- 修改 `.env.local` 后需要重启开发服务器

## 不同环境的配置

### 开发环境
```bash
# .env.local
NEXT_PUBLIC_API_BASE_URL=http://localhost:8081
```

### 生产环境
```bash
# .env.production.local
NEXT_PUBLIC_API_BASE_URL=https://api.your-domain.com
```

### Docker 部署
```bash
docker run -e NEXT_PUBLIC_API_BASE_URL=http://backend:8081 ...
```

## 配置原理

1. **环境变量优先级**:
   ```
   .env.local > .env.development > .env
   ```

2. **代码中读取配置**:
   ```typescript
   import { apiConfig } from '@/config/api.config';

   console.log(apiConfig.baseURL);   // http://localhost:8081
   console.log(apiConfig.fullURL);   // http://localhost:8081/api/v1
   ```

3. **自动应用到**:
   - ✅ Axios 客户端 (`lib/api.ts`)
   - ✅ Next.js Rewrites (`next.config.ts`)
   - ✅ 调试面板显示

## 快速测试

```bash
# 1. 设置环境变量（可选）
echo "NEXT_PUBLIC_API_BASE_URL=http://localhost:8081" > dashboard-ui/.env.local

# 2. 启动开发服务器
cd dashboard-ui
npm run dev

# 3. 查看调试信息
# 访问 http://localhost:3001/consumer-groups?id=Custodian_ApprovalFlow_ApprovalNode
# 页面底部会显示当前使用的 API URL
```

## 常见问题

**Q: 为什么修改 `.env.local` 后没有生效？**
A: 需要重启 Next.js 开发服务器（Ctrl+C 然后重新 `npm run dev`）

**Q: 生产环境如何配置？**
A: 创建 `.env.production.local` 或通过环境变量传递

**Q: 如何验证当前使用的 API URL？**
A: 开发模式下，访问任意页面，底部会显示"调试信息"面板
