# DBMQ 任务完成检查清单

## 代码开发完成后需要执行的步骤

### 1. 代码质量检查
```bash
# 格式化代码
go fmt ./...

# 静态分析
go vet ./...

# 运行所有测试
go test ./...

# 运行集成测试
go test -v ./pkg -run TestIntegration
```

### 2. 依赖管理
```bash
# 整理依赖
go mod tidy

# 验证依赖
go mod verify
```

### 3. 文档更新
- 更新相关的代码注释
- 如有API变更，更新架构文档
- 确保所有公共接口都有中文注释

### 4. 测试覆盖
- 确保新功能有对应的单元测试
- 如涉及核心功能，添加集成测试
- 验证测试覆盖率合理

### 5. 错误处理
- 检查所有错误都被正确处理
- 确保错误信息提供足够的上下文
- 验证并发安全性

### 6. 性能考虑
- 检查数据库查询是否优化
- 验证并发处理的正确性
- 确保资源正确释放

### 7. Git 提交
```bash
git add .
git commit -m "descriptive commit message"
git push
```