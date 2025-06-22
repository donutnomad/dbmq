#!/bin/bash

echo "🎯 DBMQ 消费策略演示"
echo "=================="
echo ""

# 编译程序
echo "🔨 编译演示程序..."
go build -o strategy_demo cmd/strategy_demo/main.go

if [ $? -ne 0 ]; then
    echo "❌ 编译失败"
    exit 1
fi

echo "✅ 编译成功"
echo ""

# 运行演示
echo "🚀 启动消费策略演示..."
echo ""
./strategy_demo

# 清理
echo ""
echo "🧹 清理临时文件..."
rm -f strategy_demo

echo "✅ 演示完成！" 