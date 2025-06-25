#!/bin/bash

echo "🚀 启动 DBMQ REST API 服务器和监控仪表板演示"
echo "=================================================="

# 检查是否存在 go.mod
if [ ! -f "go.mod" ]; then
    echo "❌ 错误: 请在项目根目录下运行此脚本"
    exit 1
fi

# 构建项目
echo "📦 构建项目..."
go build -o dbmq-rest-server ./cmd/rest_api_demo/

# 检查构建是否成功
if [ $? -ne 0 ]; then
    echo "❌ 构建失败"
    exit 1
fi

echo "✅ 构建成功"

# 启动服务器
echo "🌐 启动 REST API 服务器..."
echo "服务器将在 http://localhost:8080 启动"
echo ""
echo "📊 监控仪表板地址: http://localhost:8080/api/v1/dashboard"
echo "🔧 健康检查: http://localhost:8080/api/v1/health"
echo ""
echo "按 Ctrl+C 停止服务器"
echo "=================================================="

# 运行服务器
if [ -f "./dbmq-rest-server" ]; then
    ./dbmq-rest-server
else
    echo "❌ 构建的服务器文件不存在"
    exit 1
fi 