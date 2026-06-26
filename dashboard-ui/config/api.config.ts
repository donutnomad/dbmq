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

// API 路径前缀（接在挂载点父路径之后）
export const API_PREFIX = '/api';

// 挂载点最后一段（页面 URL 形如 .../<parent>/ui）
const UI_SEGMENT = 'ui';

/**
 * 运行时从浏览器当前路径推导完整 API 前缀，使后端可挂载到任意未知路径而无需重新构建。
 *
 * 规则「挂载点父路径 + 固定后缀」：
 *   页面 .../aaa/bbb/ui  ->  父路径 .../aaa/bbb  ->  API 前缀 .../aaa/bbb/dbmq/api/v1
 *
 * - 开发环境若设置了 NEXT_PUBLIC_API_BASE_URL，则优先使用它（跨域调试）。
 * - 服务端/构建阶段无 window，回退到固定前缀。
 */
export const getFullAPIURL = (): string => {
  // 跨域调试场景：显式指定了后端地址，直接拼固定前缀
  if (API_BASE_URL) {
    return `${API_BASE_URL}${API_PREFIX}`;
  }
  if (typeof window === 'undefined') {
    return API_PREFIX;
  }
  // 取当前路径，去掉查询/hash，按段拆分
  const segments = window.location.pathname.split('/').filter(Boolean);
  // 砍掉挂载点最后一段(ui)得到父路径；找不到则用整段路径兜底
  const uiIdx = segments.lastIndexOf(UI_SEGMENT);
  const parentSegments = uiIdx >= 0 ? segments.slice(0, uiIdx) : segments;
  const parent = parentSegments.length ? `/${parentSegments.join('/')}` : '';
  return `${parent}${API_PREFIX}`;
};

// 导出配置对象
export const apiConfig = {
  baseURL: API_BASE_URL,
  prefix: API_PREFIX,
  // 注意：这是模块加载时求值的快照，可能在 window 就绪前。
  // 实际请求请优先用 getFullAPIURL() 运行时求值（见 lib/api.ts 拦截器）。
  get fullURL() {
    return getFullAPIURL();
  },
  timeout: 30000,
};
