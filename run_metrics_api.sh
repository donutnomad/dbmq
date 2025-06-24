#!/bin/bash

# DBMQ监控API启动脚本
# 提供兼容Kafka UI工具的REST API接口

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_message() {
    local color=$1
    local message=$2
    echo -e "${color}${message}${NC}"
}

print_header() {
    echo
    echo "=================================================================="
    print_message $CYAN "$1"
    echo "=================================================================="
}

print_success() {
    print_message $GREEN "✅ $1"
}

print_warning() {
    print_message $YELLOW "⚠️  $1"
}

print_error() {
    print_message $RED "❌ $1"
}

print_info() {
    print_message $BLUE "ℹ️  $1"
}

# 检查依赖
check_dependencies() {
    print_header "检查依赖环境"
    
    # 检查Go环境
    if ! command -v go &> /dev/null; then
        print_error "Go环境未安装，请先安装Go 1.24+"
        exit 1
    fi
    print_success "Go环境检查通过: $(go version)"
    
#    # 检查MySQL连接
#    if ! command -v mysql &> /dev/null; then
#        print_warning "MySQL客户端未安装，无法测试数据库连接"
#    else
#        print_success "MySQL客户端可用"
#    fi
#
    # 检查端口占用
    if lsof -Pi :8080 -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_warning "端口8080已被占用，请确保没有其他服务使用该端口"
    else
        print_success "端口8080可用"
    fi
}

# 构建项目
build_project() {
    print_header "构建DBMQ监控API"
    
    # 更新依赖
    print_info "更新Go模块依赖..."
    go mod tidy
    
    # 构建metrics_demo
    print_info "构建监控演示程序..."
    cd cmd/metrics_demo
    go build -o ../../dbmq-metrics-api main.go
    cd ../..
    
    if [ -f "dbmq-metrics-api" ]; then
        print_success "构建完成: dbmq-metrics-api"
    else
        print_error "构建失败"
        exit 1
    fi
}

# 设置环境变量
setup_environment() {
    print_header "设置环境变量"
    
    # 数据库配置
    export DB_HOST=${DB_HOST:-"localhost"}
    export DB_PORT=${DB_PORT:-"3306"}
    export DB_NAME=${DB_NAME:-"dbmq"}
    export DB_USER=${DB_USER:-"root"}
    export DB_PASSWORD=${DB_PASSWORD:-"123456"}
    
    # API配置
    export API_HOST=${API_HOST:-"0.0.0.0"}
    export API_PORT=${API_PORT:-"8080"}
    export API_PREFIX=${API_PREFIX:-"/api/v1"}
    
    print_info "数据库配置: ${DB_USER}@${DB_HOST}:${DB_PORT}/${DB_NAME}"
    print_info "API配置: http://${API_HOST}:${API_PORT}${API_PREFIX}"
}

# 测试数据库连接
test_database() {
    print_header "测试数据库连接"
    
    if command -v mysql &> /dev/null; then
        if mysql -h"${DB_HOST}" -P"${DB_PORT}" -u"${DB_USER}" -p"${DB_PASSWORD}" -e "USE ${DB_NAME}; SELECT 1;" &> /dev/null; then
            print_success "数据库连接测试通过"
        else
            print_warning "数据库连接测试失败，请检查配置"
            print_info "你可以手动测试: mysql -h${DB_HOST} -P${DB_PORT} -u${DB_USER} -p${DB_PASSWORD} ${DB_NAME}"
        fi
    else
        print_info "跳过数据库连接测试（MySQL客户端不可用）"
    fi
}

# 启动API服务
start_api() {
    print_header "启动DBMQ监控API服务"
    
    print_info "正在启动服务..."
    print_info "按Ctrl+C停止服务"
    echo
    
    # 启动服务
    ./dbmq-metrics-api
}

# 显示使用说明
show_usage() {
    print_header "DBMQ监控API使用说明"
    
    echo
    print_message $PURPLE "🌐 REST API端点:"
    echo "  健康检查: http://localhost:8080/api/v1/health"
    echo "  集群指标: http://localhost:8080/api/v1/clusters/dbmq-cluster/metrics"
    echo "  Topic列表: http://localhost:8080/api/v1/clusters/dbmq-cluster/topics"
    echo "  消费组列表: http://localhost:8080/api/v1/clusters/dbmq-cluster/consumer-groups"
    echo "  Broker信息: http://localhost:8080/api/v1/clusters/dbmq-cluster/brokers"
    echo "  DBMQ统计: http://localhost:8080/api/v1/dbmq/stats"
    echo
    
    print_message $PURPLE "🔧 兼容Kafka UI的接口:"
    echo "  Kafka REST Proxy风格: http://localhost:8080/api/v1/topics"
    echo "  Spring Boot Actuator: http://localhost:8080/api/v1/actuator/health"
    echo
    
    print_message $PURPLE "🧪 测试命令:"
    echo "  curl http://localhost:8080/api/v1/health"
    echo "  curl http://localhost:8080/api/v1/clusters"
    echo "  curl http://localhost:8080/api/v1/dbmq/stats"
    echo
    
    print_message $PURPLE "📖 更多信息:"
    echo "  查看文档: docs/kafka_ui_compatibility.md"
    echo "  配置Kafka UI: 参考文档中的配置示例"
    echo
}

# 清理函数
cleanup() {
    print_header "清理资源"
    
    if [ -f "dbmq-metrics-api" ]; then
        rm -f dbmq-metrics-api
        print_success "清理构建文件"
    fi
}

# 主函数
main() {
    print_header "🚀 DBMQ监控API启动器"
    print_info "兼容Kafka UI、Conduktor、Redpanda Console等管理工具"
    
    # 解析命令行参数
    case "${1:-start}" in
        "start")
            check_dependencies
            setup_environment
            build_project
            test_database
            show_usage
            start_api
            ;;
        "build")
            check_dependencies
            build_project
            print_success "构建完成，使用 './run_metrics_api.sh start' 启动服务"
            ;;
        "clean")
            cleanup
            print_success "清理完成"
            ;;
        "test")
            setup_environment
            test_database
            ;;
        "help"|"-h"|"--help")
            echo "用法: $0 [命令]"
            echo
            echo "命令:"
            echo "  start   - 构建并启动API服务 (默认)"
            echo "  build   - 仅构建项目"
            echo "  clean   - 清理构建文件"
            echo "  test    - 测试数据库连接"
            echo "  help    - 显示此帮助信息"
            echo
            echo "环境变量:"
            echo "  DB_HOST     - 数据库主机 (默认: localhost)"
            echo "  DB_PORT     - 数据库端口 (默认: 3306)"
            echo "  DB_NAME     - 数据库名称 (默认: dbmq)"
            echo "  DB_USER     - 数据库用户 (默认: root)"
            echo "  DB_PASSWORD - 数据库密码 (默认: password)"
            echo "  API_HOST    - API监听地址 (默认: 0.0.0.0)"
            echo "  API_PORT    - API监听端口 (默认: 8080)"
            echo
            ;;
        *)
            print_error "未知命令: $1"
            print_info "使用 '$0 help' 查看帮助信息"
            exit 1
            ;;
    esac
}

# 捕获中断信号
trap cleanup EXIT

# 运行主函数
main "$@"