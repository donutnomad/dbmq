# DBMQ 代码结构

## 目录组织
```
dbmq/
├── internal/           # 内部包，不对外暴露
│   ├── dal/           # 数据访问层
│   │   ├── dal.go     # 数据库操作函数
│   │   └── dal_test.go
│   └── db/            # 数据库连接和模式
│       ├── db.go      # MySQL和Redis连接初始化
│       └── schema.go  # 数据库表结构定义
├── pkg/               # 公共包，对外暴露的API
│   ├── types/         # 类型定义
│   ├── errors/        # 错误类型定义
│   ├── producer.go    # 生产者实现
│   ├── consumer.go    # 消费者实现
│   ├── coordinator.go # 协调器实现
│   ├── utils.go       # 工具函数
│   ├── set.go         # 集合数据结构
│   └── *_test.go      # 各种测试文件
└── 设计架构文档.md    # 详细的架构设计文档
```

## 核心组件
1. **Producer**: 消息生产者，支持分区策略和Redis通知
2. **Consumer**: 消息消费者，支持消费组、自动重新均衡
3. **Coordinator**: 协调器，管理消费组和分区分配
4. **DAL**: 数据访问层，封装所有数据库操作
5. **Types**: 核心数据结构定义，包括ConsumerHeartbeat等