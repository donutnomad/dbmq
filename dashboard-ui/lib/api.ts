import axios from 'axios';
import { APIResponse, DashboardData, TopicMetrics, ConsumerGroupMetrics, NewTopicRequest, Message, PartitionStats, ManualAssignment, CreateManualAssignmentRequest, ClusterInfo, ClusterMetricsDetail, BrokerInfo, DBMQStats, RMQConsumerInfo, ResendMessagesRequest, ResendMessagesResponse } from './types';
import { apiConfig } from '@/config/api.config';

// 创建axios实例
const apiClient = axios.create({
  baseURL: apiConfig.fullURL,
  timeout: apiConfig.timeout,
  headers: {
    'Content-Type': 'application/json',
  },
});

// API客户端类
export class DBMQAPIClient {
  // 获取仪表板数据
  static async getDashboardData(): Promise<DashboardData> {
    const response = await apiClient.get<APIResponse<DashboardData>>('/dashboard/data');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch dashboard data');
  }

  // 获取所有Topics
  static async getTopics(includePartitionStats = false): Promise<TopicMetrics[]> {
    const url = includePartitionStats
      ? '/topics?includePartitionStats=true'
      : '/topics';
    const response = await apiClient.get<APIResponse<TopicMetrics[]>>(url);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch topics');
  }

  // 获取单个Topic信息
  static async getTopic(topicName: string): Promise<TopicMetrics> {
    const response = await apiClient.get<APIResponse<TopicMetrics>>(`/topics/${topicName}`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch topic');
  }

  // 创建Topic
  static async createTopic(topic: NewTopicRequest): Promise<void> {
    const response = await apiClient.post<APIResponse>('/topics', topic);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to create topic');
    }
  }

  // 删除Topic
  static async deleteTopic(topicName: string): Promise<void> {
    const response = await apiClient.delete<APIResponse>(`/topics/${topicName}`);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to delete topic');
    }
  }

  // 获取所有消费组
  static async getConsumerGroups(): Promise<ConsumerGroupMetrics[]> {
    const response = await apiClient.get<APIResponse<ConsumerGroupMetrics[]>>('/consumer-groups');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch consumer groups');
  }

  // 获取单个消费组信息
  static async getConsumerGroup(groupId: string): Promise<ConsumerGroupMetrics> {
    try {
      // 获取基本信息
      const response = await apiClient.get<APIResponse<ConsumerGroupMetrics>>(`/consumer-groups/${groupId}`);
      if (!response.data.success || !response.data.data) {
        throw new Error(response.data.error || 'Failed to fetch consumer group');
      }

      const groupData = response.data.data;

      // 获取扩展信息（如果有）
      try {
        const extendedResponse = await apiClient.get<APIResponse<any>>(`/consumer-groups/${groupId}/extended`);
        if (extendedResponse.data.success && extendedResponse.data.data) {
          Object.assign(groupData, extendedResponse.data.data);
        }
      } catch (extErr) {
        console.warn('Extended consumer group API not available:', extErr);
      }

      return groupData;
    } catch (err) {
      console.error('Error fetching consumer group details:', err);
      throw err;
    }
  }

  // 触发消费组 rebalance
  static async triggerRebalance(groupId: string): Promise<void> {
    const response = await apiClient.post<APIResponse<{ message: string }>>(
      `/consumer-groups/${encodeURIComponent(groupId)}/rebalance`
    );
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to trigger rebalance');
    }
  }

  // 获取Topic消息
  static async getTopicMessages(
    topicName: string,
    params?: {
      partition?: number;
      offset?: number;
      limit?: number;
      search?: string;
      from_time?: string;
      to_time?: string;
    }
  ): Promise<{ messages: Message[]; total: number }> {
    const searchParams = new URLSearchParams();
    if (params) {
      Object.entries(params).forEach(([key, value]) => {
        if (value !== undefined) {
          searchParams.set(key, value.toString());
        }
      });
    }

    const response = await apiClient.get<APIResponse<{ messages: Message[]; total: number }>>(
      `/topics/${topicName}/messages?${searchParams.toString()}`
    );
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch messages');
  }

  // 发送消息到Topic
  // NOTE: 后端暂未实现此 API
  static async sendMessage(topicName: string, data: {
    key?: string | null;
    value: string;
    headers?: Record<string, unknown>;
  }): Promise<void> {
    throw new Error('sendMessage API is not implemented in backend');
    // 原实现保留供后续实现:
    // const response = await apiClient.post<APIResponse>(
    //   `/topics/${encodeURIComponent(topicName)}/messages`,
    //   data
    // );
    // if (!response.data.success) {
    //   throw new Error(response.data.error || 'Failed to send message');
    // }
  }

  // 获取分区统计信息
  static async getPartitionStats(topicName: string, partitionId: number): Promise<PartitionStats> {
    const response = await apiClient.get<APIResponse<PartitionStats>>(
      `/topics/${topicName}/partitions/${partitionId}/stats`
    );
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch partition stats');
  }

  // 健康检查
  static async healthCheck(): Promise<Record<string, unknown>> {
    const response = await apiClient.get<APIResponse>('/health');
    if (response.data.success && response.data.data) {
      return response.data.data as Record<string, unknown>;
    }
    throw new Error(response.data.error || 'Health check failed');
  }

  // 获取手动分配列表
  static async getManualAssignments(groupId: string): Promise<ManualAssignment[]> {
    const response = await apiClient.get<APIResponse<ManualAssignment[]>>(
      `/manual-assignments/?group_id=${encodeURIComponent(groupId)}`
    );
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch manual assignments');
  }

  // 创建手动分配
  static async createManualAssignment(data: CreateManualAssignmentRequest): Promise<ManualAssignment> {
    const response = await apiClient.post<APIResponse<ManualAssignment>>('/manual-assignments/', data);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to create manual assignment');
  }

  // 删除手动分配
  static async deleteManualAssignment(id: number): Promise<void> {
    const response = await apiClient.delete<APIResponse>(`/manual-assignments/${id}`);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to delete manual assignment');
    }
  }

  // 获取集群列表
  static async getClusters(): Promise<ClusterInfo[]> {
    const response = await apiClient.get<APIResponse<ClusterInfo[]>>('/clusters');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch clusters');
  }

  // 获取集群指标
  static async getClusterMetrics(clusterId: string): Promise<ClusterMetricsDetail> {
    const response = await apiClient.get<APIResponse<ClusterMetricsDetail>>(`/clusters/${clusterId}/metrics`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch cluster metrics');
  }

  // 获取 Broker 列表
  static async getClusterBrokers(clusterId: string): Promise<BrokerInfo[]> {
    const response = await apiClient.get<APIResponse<BrokerInfo[]>>(`/clusters/${clusterId}/brokers`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch brokers');
  }

  // 获取 DBMQ 统计信息
  static async getDBMQStats(): Promise<DBMQStats> {
    const response = await apiClient.get<APIResponse<DBMQStats>>('/stats');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch DBMQ stats');
  }

  // 获取 Topic 指标
  static async getTopicMetrics(topicName: string): Promise<TopicMetrics> {
    const response = await apiClient.get<APIResponse<TopicMetrics>>(`/topics/${topicName}/metrics`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch topic metrics');
  }

  // 获取 RMQ 消费者列表
  static async getRMQConsumers(): Promise<RMQConsumerInfo[]> {
    const response = await apiClient.get<APIResponse<RMQConsumerInfo[]>>('/rmq/consumers');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch RMQ consumers');
  }

  // 暂停 RMQ 消费者
  static async pauseConsumer(consumerId: string): Promise<void> {
    const response = await apiClient.post<APIResponse>(`/rmq/consumers/${encodeURIComponent(consumerId)}/pause`);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to pause consumer');
    }
  }

  // 恢复 RMQ 消费者
  static async resumeConsumer(consumerId: string): Promise<void> {
    const response = await apiClient.post<APIResponse>(`/rmq/consumers/${encodeURIComponent(consumerId)}/resume`);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to resume consumer');
    }
  }

  // 重发消息
  static async resendMessages(request: ResendMessagesRequest): Promise<ResendMessagesResponse> {
    const response = await apiClient.post<APIResponse<ResendMessagesResponse>>('/messages/resend', request);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to resend messages');
  }
}

export default DBMQAPIClient; 