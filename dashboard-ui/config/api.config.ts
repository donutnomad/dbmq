/**
 * API 配置中心
 * 所有 API 相关的 URL 配置都在这里统一管理
 */

// 后端 API 基础 URL
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8081';

// API 路径前缀
export const API_PREFIX = '/dbmq/api/v1';

// 完整的 API URL
export const getFullAPIURL = () => {
  return `${API_BASE_URL}${API_PREFIX}`;
};

// 导出配置对象
export const apiConfig = {
  baseURL: API_BASE_URL,
  prefix: API_PREFIX,
  fullURL: getFullAPIURL(),
  timeout: 30000,
};
