/**
 * API 配置中心
 * 所有 API 相关的 URL 配置都在这里统一管理
 *
 * 默认使用相对路径（空字符串），这样构建后的静态文件嵌入 Go 二进制时，
 * 会自动使用当前页面的 host:port 请求 API（同源）。
 * 开发时可通过 .env.local 设置 NEXT_PUBLIC_API_BASE_URL=http://localhost:8081 来跨域调试。
 */

// 后端 API 基础 URL（默认为空 = 相对路径，即同源请求）
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || '';

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
