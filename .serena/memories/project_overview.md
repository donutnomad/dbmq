# DBMQ 项目概述

## 项目目的
DBMQ是一个基于MySQL的轻量级消息队列系统，旨在提供与Apache Kafka高度兼容的API和核心行为。主要目标是让开发人员能够以低成本、甚至无缝地从Kafka迁移到生产级集群。

## 技术栈
- **语言**: Go 1.24
- **数据库**: MySQL (作为核心存储和事实来源)
- **缓存**: Redis (可选，用于实时通知优化)
- **依赖管理**: Go Modules
- **主要依赖**:
  - GORM v1.30.0 (ORM框架)
  - Redis Go客户端 v9.10.0
  - UUID生成库
  - Testify (测试框架)

## 核心概念
- **Topic**: 消息的逻辑分类
- **Partition**: Topic的物理分组，实现并行处理和顺序保证
- **Offset**: 分区内消息的唯一、单调递增ID
- **Producer**: 消息生产者
- **Consumer**: 消息消费者，支持消费组模式
- **Coordinator**: 中央服务集群，负责管理所有消费组和再均衡