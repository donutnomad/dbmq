import type { NextConfig } from "next";

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
          destination: 'http://localhost:8080/api/:path*',
        },
      ];
    }
    return [];
  },
  
  // 配置基础路径（如果需要部署到子路径）
  basePath: process.env.NODE_ENV === 'production' ? '' : '',
  
  // 配置环境变量
  env: {
    DBMQ_API_BASE: process.env.DBMQ_API_BASE || 'http://localhost:8080',
  }
};

export default nextConfig;
