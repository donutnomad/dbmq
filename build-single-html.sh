#!/bin/bash
# 构建 dashboard-ui 并将静态文件复制到 dbmqapi/static/dashboard 供 Go embed 使用
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UI_DIR="$SCRIPT_DIR/dashboard-ui"
OUT_DIR="$UI_DIR/out"
STATIC_DIR="$SCRIPT_DIR/dbmqapi/static/dashboard"

# 1. 构建静态文件
echo "==> 构建 Next.js 静态导出..."
cd "$UI_DIR"
DASHBOARD_BASE_PATH=/console/dbmq/ui npm run build

if [ ! -d "$OUT_DIR" ]; then
    echo "错误: 构建失败，out 目录不存在"
    exit 1
fi

# 2. 复制到 dbmqapi/static/dashboard
echo "==> 复制静态文件到 dbmqapi/static/dashboard..."
rm -rf "$STATIC_DIR"
cp -r "$OUT_DIR" "$STATIC_DIR"

echo "==> 完成！静态文件已复制到 dbmqapi/static/dashboard/"
echo "    现在可以 go build 将 dashboard 嵌入到二进制文件中"
