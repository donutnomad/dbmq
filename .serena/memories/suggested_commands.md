# DBMQ 开发常用命令

## Go 开发命令
```bash
# 运行测试
go test ./...

# 运行特定包的测试
go test ./pkg
go test ./internal/dal

# 运行带详细输出的测试
go test -v ./...

# 运行集成测试
go test -v ./pkg -run TestIntegration

# 构建项目
go build ./...

# 格式化代码
go fmt ./...

# 静态检查
go vet ./...

# 下载依赖
go mod download

# 整理依赖
go mod tidy

# 查看依赖
go mod graph
```

## 系统命令 (macOS)
```bash
# 查看文件
ls -la
cat filename

# 搜索文件内容
grep -r "pattern" .
find . -name "*.go" -exec grep -l "pattern" {} \;

# Git 操作
git status
git add .
git commit -m "message"
git push

# 进程管理
ps aux | grep process_name
kill -9 PID
```

## 数据库相关
```bash
# 连接MySQL (需要根据实际配置调整)
mysql -h localhost -u root -p

# 查看Redis
redis-cli
redis-cli ping
```