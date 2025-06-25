#!/bin/bash

echo "🚀 DBMQ 超级演示启动脚本"
echo "=========================="
echo

echo "🎬 启动超级演示..."
echo "⏰ 演示将运行20秒后自动关闭"
echo "📊 请观察控制台输出了解消息流转过程"
echo

# 运行演示
go run cmd/super_demo/main.go

echo
echo "🏁 演示完成！"
echo "💡 提示："
echo "   - 可以多次运行此脚本"
echo "   - 数据会持久化到MySQL数据库"
echo "   - 查看 examples/README.md 了解更多详情" 