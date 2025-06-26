// API响应类型
export interface APIResponse<T = unknown> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// Topic相关类型
export interface TopicMetrics {
  name?: string;
  topicName?: string;
  partitionCount?: number;
  partitions?: PartitionInfo[];
  messageCount?: number;
  status?: string;
  bytesIn?: number;
  bytesOut?: number;
  messagesIn?: number;
  messagesOut?: number;
  description?: string;
  createdAt?: string;
  sizeBytes?: number;
}

// 分区信息类型
export interface PartitionInfo {
  partition: number;
  leader?: number;
  replicas?: number[];
  isr?: number[];
}

// 分区延迟信息
export interface PartitionLag {
  topic?: string;
  partition?: number;
  currentOffset?: number;
  latestOffset?: number;
  lag?: number;
  updatedAt?: string;
  metadata?: string;
  generationId?: number;
}

// 分区分配信息
export interface PartitionAssignmentInfo {
  topic: string;
  partition: number;
}

// 消费组相关类型
export interface ConsumerGroupMetrics {
  groupId?: string;
  name?: string;
  state?: string;
  status?: string;
  memberCount?: number;
  members?: GroupMember[];
  lag?: number;
  totalLag?: number;
  generationId?: string | number;
  assignedTopics?: string[];
  partitionLags?: PartitionLag[];
  commitMode?: 'auto' | 'manual' | string;
  coordinator?: string;
  lastActivity?: string;
  assignmentStrategy?: string;
  createdAt?: number;
}

// 消费组成员类型
export interface GroupMember {
  memberId: string;
  clientId?: string;
  host?: string;
  assignment?: PartitionAssignmentInfo[];
  lastHeartbeat?: string;
  subscribedTopics?: string[];
}

// 仪表板数据类型
export interface DashboardData {
  topics: TopicMetrics[];
  consumerGroups: ConsumerGroupMetrics[];
  system: {
    uptime: number;
    version: string;
  };
  timestamp: string;
}

// 集群指标类型
export interface ClusterMetrics {
  brokerId?: string;
  topicCount?: number;
  partitionCount?: number;
  messageCount?: number;
}

// 消息类型
export interface Message {
  id: string;
  topic: string;
  partition: number;
  offset: number;
  key?: string;
  value: string;
  timestamp: string;
  size: number;
  headers?: Record<string, string>;
}

// Topic创建请求类型
export interface NewTopicRequest {
  name: string;
  partitions?: number;
  replicationFactor?: number;
  configs?: Record<string, string>;
}

// 系统信息类型
export interface SystemInfo {
  uptime: number;
  version: string;
  timestamp: string;
}

export interface SystemMetrics {
  uptime: number;
  version: string;
  load: number;
} 