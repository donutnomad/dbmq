#!/bin/bash

# DBMQ 超级演示启动脚本
# 自动检查依赖并运行演示

echo "🚀 DBMQ 超级演示启动脚本"
echo "=========================="
echo

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "❌ Go未安装，请先安装Go 1.21+"
    exit 1
fi

echo "✅ Go环境检查通过: $(go version)"

# 检查MySQL连接
echo "🔍 检查MySQL连接..."
if command -v mysql &> /dev/null; then
    # 尝试连接MySQL
    if mysql -h localhost -P 3306 -u root -ppassword -e "SELECT 1;" &> /dev/null; then
        echo "✅ MySQL连接正常"
    else
        echo "⚠️  MySQL连接失败，请检查以下配置:"
        echo "   - 主机: localhost:3306"
        echo "   - 用户: root"
        echo "   - 密码: password"
        echo "   - 如需修改配置，请编辑 examples/super_demo.go"
        echo
        echo "继续运行演示（如果配置正确，演示会自动创建数据库）..."
    fi
else
    echo "⚠️  未找到mysql命令，请确保MySQL已安装并运行"
fi

# 检查Redis连接（可选）
echo "🔍 检查Redis连接..."
if command -v redis-cli &> /dev/null; then
    if redis-cli -h localhost -p 6379 ping &> /dev/null; then
        echo "✅ Redis连接正常（将启用实时通知优化）"
    else
        echo "⚠️  Redis连接失败，将使用轮询模式"
    fi
else
    echo "ℹ️  未找到redis-cli，将使用轮询模式"
fi

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