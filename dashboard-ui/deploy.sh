#!/bin/bash

# DBMQ Dashboard UI 部署脚本
# 此脚本将构建Next.js项目并将静态文件复制到DBMQ后端的静态资源目录

set -e

echo "🔨 开始构建 Dashboard UI..."

# 构建项目
npm run build

echo "✅ 构建完成！"

# 检查是否提供了目标目录参数
if [ $# -eq 0 ]; then
    echo "📁 静态文件已生成到 'out' 目录"
    echo "📖 使用方法:"
    echo "   ./deploy.sh [目标目录]"
    echo ""
    echo "📖 示例:"
    echo "   ./deploy.sh ../templates/static-new"
    echo "   ./deploy.sh /var/www/dbmq-dashboard"
    echo ""
    echo "📋 生成的文件:"
    ls -la out/
    exit 0
fi

TARGET_DIR="$1"

echo "📁 目标目录: $TARGET_DIR"

# 创建目标目录（如果不存在）
mkdir -p "$TARGET_DIR"

# 复制静态文件
echo "📂 复制静态文件到 $TARGET_DIR ..."
cp -r out/* "$TARGET_DIR/"

echo "✅ 部署完成！"
echo ""
echo "📁 文件已复制到: $TARGET_DIR"
echo "🌐 现在可以通过 DBMQ 后端访问 Dashboard UI"
echo ""
echo "💡 提示："
echo "   1. 确保 DBMQ 后端服务正在运行"
echo "   2. 访问 http://localhost:8080/static/index.html 查看 Dashboard"
echo "   3. 或者配置你的 Web 服务器指向这些静态文件" 