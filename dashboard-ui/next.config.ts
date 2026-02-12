import type { NextConfig } from "next";

// API 配置 - 统一的配置源
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8081';

const nextConfig: NextConfig = {
  // 注释掉静态导出配置，因为有动态路由
  output: 'export',
  // trailingSlash: true,

  // 图片优化配置
  images: {
    unoptimized: true
  },

  // 配置API重写（开发环境使用）
  async rewrites() {
    if (process.env.NODE_ENV === 'development') {
      return [
        {
          source: '/api/:path*',
          destination: `${API_BASE_URL}/api/:path*`,
        },
      ];
    }
    return [];
  },

  // 配置基础路径（嵌入 Go 二进制时使用 /dashboard）
  basePath: process.env.DASHBOARD_BASE_PATH || '',
};

export default nextConfig;
