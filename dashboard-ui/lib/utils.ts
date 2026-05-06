import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

// 合并 CSS 类名
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

// 格式化数字显示
export function formatNumber(num: number | undefined | null): string {
  if (num === undefined || num === null) return '--';
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  } else if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num.toString();
}

// 格式化文件大小
export function formatBytes(bytes: number | undefined | null): string {
  if (bytes === undefined || bytes === null) return '--';
  if (bytes === 0) return '0 B';
  
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// 格式化运行时间
export function formatUptime(value: number | string | undefined | null): string {
  if (value === undefined || value === null || value === '') return '--';

  const seconds = typeof value === 'string'
    ? Number(value.replace(/s$/i, ''))
    : value;

  if (!Number.isFinite(seconds) || seconds < 0) return '--';

  const totalSeconds = Math.floor(seconds);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const remainingSeconds = totalSeconds % 60;

  if (days > 0) {
    return `${days}天 ${hours}小时 ${minutes}分钟`;
  }
  if (hours > 0) {
    return `${hours}小时 ${minutes}分钟`;
  }
  if (minutes > 0) {
    return `${minutes}分钟 ${remainingSeconds}秒`;
  }
  return `${remainingSeconds}秒`;
}

// 格式化时间戳
export function formatTimestamp(timestamp: number | string | Date): string {
  const date = typeof timestamp === 'number' 
    ? new Date(timestamp) 
    : typeof timestamp === 'string' 
      ? new Date(timestamp)
      : timestamp;
      
  const base = new Intl.DateTimeFormat('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false
  }).format(date);
  const ms = String(date.getMilliseconds()).padStart(3, '0');
  return `${base}.${ms}`;
}

// 获取状态对应的颜色类
export function getStatusColor(status: string | undefined): string {
  switch (status?.toLowerCase()) {
    case 'active':
    case 'running':
    case 'online':
    case 'healthy':
    case 'stable':
      return 'text-green-600 bg-green-100';
    case 'warning':
    case 'degraded':
      return 'text-yellow-600 bg-yellow-100';
    case 'error':
    case 'failed':
    case 'offline':
    case 'dead':
      return 'text-red-600 bg-red-100';
    case 'empty':
    case 'idle':
      return 'text-blue-600 bg-blue-100';
    default:
      return 'text-gray-600 bg-gray-100';
  }
}

// 获取状态文本
export function getStatusText(status: string | undefined): string {
  switch (status?.toLowerCase()) {
    case 'active':
      return '活跃';
    case 'running':
      return '运行中';
    case 'online':
      return '在线';
    case 'healthy':
      return '健康';
    case 'stable':
      return '稳定';
    case 'warning':
      return '警告';
    case 'degraded':
      return '降级';
    case 'error':
      return '错误';
    case 'failed':
      return '失败';
    case 'offline':
      return '离线';
    case 'dead':
      return '停止';
    case 'empty':
      return '空闲';
    case 'idle':
      return '闲置';
    default:
      return status || '未知';
  }
}

// 防抖函数
export function debounce<T extends (...args: unknown[]) => unknown>(
  func: T,
  wait: number
): (...args: Parameters<T>) => void {
  let timeout: NodeJS.Timeout;
  return (...args: Parameters<T>) => {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

// 节流函数
export function throttle<T extends (...args: unknown[]) => unknown>(
  func: T,
  limit: number
): (...args: Parameters<T>) => void {
  let inThrottle: boolean;
  return (...args: Parameters<T>) => {
    if (!inThrottle) {
      func(...args);
      inThrottle = true;
      setTimeout(() => (inThrottle = false), limit);
    }
  };
}

// 复制到剪贴板
export async function copyToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch (err) {
    console.error('Failed to copy text: ', err);
    return false;
  }
} 
