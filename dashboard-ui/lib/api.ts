import axios from 'axios';
import { APIResponse, DashboardData, TopicMetrics, ConsumerGroupMetrics, NewTopicRequest, Message, PartitionStats } from './types';

// 获取API基础URL
const getAPIBaseURL = () => {
  if (typeof window !== 'undefined') {
    // 客户端环境
    return process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8080';
  }
  // 服务端环境
  return process.env.DBMQ_API_BASE || 'http://localhost:8080';
};

const API_PREFIX = '/api/v1';

// 创建axios实例
const apiClient = axios.create({
  baseURL: getAPIBaseURL() + API_PREFIX,
  timeout: 30000,
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
      ? '/clusters/dbmq-cluster/topics?includePartitionStats=true'
      : '/clusters/dbmq-cluster/topics';
    const response = await apiClient.get<APIResponse<TopicMetrics[]>>(url);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch topics');
  }

  // 获取单个Topic信息
  static async getTopic(topicName: string): Promise<TopicMetrics> {
    const response = await apiClient.get<APIResponse<TopicMetrics>>(`/clusters/dbmq-cluster/topics/${topicName}`);
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch topic');
  }

  // 创建Topic
  static async createTopic(topic: NewTopicRequest): Promise<void> {
    const response = await apiClient.post<APIResponse>('/clusters/dbmq-cluster/topics', topic);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to create topic');
    }
  }

  // 删除Topic
  static async deleteTopic(topicName: string): Promise<void> {
    const response = await apiClient.delete<APIResponse>(`/clusters/dbmq-cluster/topics/${topicName}`);
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to delete topic');
    }
  }

  // 获取所有消费组
  static async getConsumerGroups(): Promise<ConsumerGroupMetrics[]> {
    const response = await apiClient.get<APIResponse<ConsumerGroupMetrics[]>>('/clusters/dbmq-cluster/consumer-groups');
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch consumer groups');
  }

  // 获取单个消费组信息
  static async getConsumerGroup(groupId: string): Promise<ConsumerGroupMetrics> {
    try {
      // 先获取基本信息
      const response = await apiClient.get<APIResponse<ConsumerGroupMetrics>>(`/clusters/dbmq-cluster/consumer-groups/${groupId}`);
      if (!response.data.success || !response.data.data) {
        throw new Error(response.data.error || 'Failed to fetch consumer group');
      }
      
      // 获取详细指标信息
      const metricsResponse = response
      // 合并基本信息和详细指标
      const groupData = response.data.data;
      if (metricsResponse.data.success && metricsResponse.data.data) {
        Object.assign(groupData, metricsResponse.data.data);
      }
      
      // 获取扩展信息（如果有）
      try {
        const extendedResponse = await apiClient.get<APIResponse<any>>(`/dbmq/consumer-groups/${groupId}/extended`);
        if (extendedResponse.data.success && extendedResponse.data.data) {
          // 合并扩展信息
          Object.assign(groupData, extendedResponse.data.data);
        }
      } catch (extErr) {
        // 扩展API可能不存在，忽略错误
        console.warn('Extended consumer group API not available:', extErr);
      }
      
      return groupData;
    } catch (err) {
      console.error('Error fetching consumer group details:', err);
      throw err;
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
      `/dbmq/topics/${topicName}/messages?${searchParams.toString()}`
    );
    if (response.data.success && response.data.data) {
      return response.data.data;
    }
    throw new Error(response.data.error || 'Failed to fetch messages');
  }

  // 发送消息到Topic
  static async sendMessage(topicName: string, data: {
    key?: string | null;
    value: string;
    headers?: Record<string, unknown>;
  }): Promise<void> {
    const response = await apiClient.post<APIResponse>(
      `/clusters/dbmq-cluster/topics/${encodeURIComponent(topicName)}/messages`,
      data
    );
    if (!response.data.success) {
      throw new Error(response.data.error || 'Failed to send message');
    }
  }

  // 获取分区统计信息
  static async getPartitionStats(topicName: string, partitionId: number): Promise<PartitionStats> {
    const response = await apiClient.get<APIResponse<PartitionStats>>(
      `/dbmq/topics/${topicName}/partitions/${partitionId}/stats`
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
}

export default DBMQAPIClient; 