'use client';

import { useState, useEffect, useCallback } from 'react';
import { DBMQAPIClient } from '@/lib/api';
import { RMQConsumerInfo } from '@/lib/types';
import { formatTimestamp } from '@/lib/utils';
import { StatCard } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  ArrowLeft,
  RefreshCw,
  Radio,
  Users,
  Pause,
  Play,
} from 'lucide-react';
import Link from 'next/link';

export default function ConsumersPage() {
  const [consumers, setConsumers] = useState<RMQConsumerInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [actionLoading, setActionLoading] = useState<string | null>(null);

  const loadConsumers = useCallback(async () => {
    try {
      const data = await DBMQAPIClient.getRMQConsumers();
      setConsumers(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load consumers:', err);
      setError(err instanceof Error ? err.message : '加载消费者列表失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadConsumers();
  }, [loadConsumers]);

  const handlePause = async (consumerId: string) => {
    setActionLoading(consumerId);
    try {
      await DBMQAPIClient.pauseConsumer(consumerId);
      await loadConsumers();
    } catch (err) {
      console.error('Failed to pause consumer:', err);
    } finally {
      setActionLoading(null);
    }
  };

  const handleResume = async (consumerId: string) => {
    setActionLoading(consumerId);
    try {
      await DBMQAPIClient.resumeConsumer(consumerId);
      await loadConsumers();
    } catch (err) {
      console.error('Failed to resume consumer:', err);
    } finally {
      setActionLoading(null);
    }
  };

  const onlineCount = consumers.filter(c => !c.offline && !c.paused).length;
  const pausedCount = consumers.filter(c => c.paused).length;

  const getStatusDot = (consumer: RMQConsumerInfo) => {
    if (consumer.paused) return 'bg-yellow-500';
    if (consumer.offline) return 'bg-red-500';
    return 'bg-green-500';
  };

  const getStatusBadge = (consumer: RMQConsumerInfo) => {
    if (consumer.paused) return <Badge variant="warning">暂停</Badge>;
    if (consumer.offline) return <Badge variant="error">离线</Badge>;
    return <Badge variant="success">在线</Badge>;
  };

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载消费者列表中...</p>
        </div>
      </div>
    );
  }

  if (error && consumers.length === 0) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
            <p className="font-bold">错误</p>
            <p>{error}</p>
          </div>
          <Button onClick={loadConsumers} variant="primary">
            重试
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 text-sm">
      {/* 头部 */}
      <header className="bg-white shadow-sm">
        <div className="max-w-[98%] mx-auto px-4">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <Link
                href="/"
                className="inline-flex items-center text-gray-600 hover:text-blue-600 transition-colors"
              >
                <ArrowLeft className="h-4 w-4 mr-1" />
                返回
              </Link>
              <div>
                <h1 className="text-xl font-medium text-gray-900">消费者管理</h1>
                <p className="text-sm text-gray-500 mt-0.5">RMQ 消费者实例管理</p>
              </div>
            </div>
            <Button onClick={loadConsumers} size="sm" variant="outline" disabled={loading}>
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>
      </header>

      <main className="max-w-[98%] mx-auto px-4 py-4">
        {/* 统计卡片 */}
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mb-4">
          <StatCard
            title="总消费者数"
            value={consumers.length}
            icon={<Radio className="h-5 w-5 text-blue-500" />}
            className="bg-blue-50 border-none"
          />
          <StatCard
            title="在线数"
            value={onlineCount}
            icon={<Users className="h-5 w-5 text-green-500" />}
            className="bg-green-50 border-none"
          />
          <StatCard
            title="暂停数"
            value={pausedCount}
            icon={<Pause className="h-5 w-5 text-yellow-500" />}
            className="bg-yellow-50 border-none"
          />
        </div>

        {/* 消费者表格 */}
        <div className="bg-white rounded-lg shadow-sm">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    消费者 ID
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    队列名
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    预取数
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    独占
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    最后心跳
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    状态
                  </th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    操作
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {consumers.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-4 py-4 text-center text-gray-500">
                      暂无消费者数据
                    </td>
                  </tr>
                ) : (
                  consumers.map((consumer) => (
                    <tr
                      key={consumer.consumerId}
                      className={`hover:bg-gray-50 transition-colors ${
                        consumer.offline && !consumer.paused ? 'bg-red-50' : ''
                      }`}
                    >
                      <td className="px-4 py-2 whitespace-nowrap">
                        <div className="flex items-center">
                          <div className={`w-2 h-2 rounded-full mr-2 ${getStatusDot(consumer)}`} />
                          <span className="text-gray-900">{consumer.consumerId}</span>
                        </div>
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap text-gray-600">
                        {consumer.queueName}
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap text-gray-600">
                        {consumer.prefetchCount}
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap">
                        {consumer.exclusive ? (
                          <Badge variant="warning">独占</Badge>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap text-gray-600">
                        {consumer.lastHeartbeat ? formatTimestamp(consumer.lastHeartbeat) : '--'}
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap">
                        {getStatusBadge(consumer)}
                      </td>
                      <td className="px-4 py-2 whitespace-nowrap">
                        {consumer.offline ? (
                          <span className="text-xs text-gray-400">不可用</span>
                        ) : consumer.paused ? (
                          <Button
                            size="xs"
                            variant="outline"
                            className="text-green-600 hover:text-green-800"
                            onClick={() => handleResume(consumer.consumerId)}
                            disabled={actionLoading === consumer.consumerId}
                          >
                            <Play className="h-3 w-3 mr-1" />
                            恢复
                          </Button>
                        ) : (
                          <Button
                            size="xs"
                            variant="outline"
                            className="text-yellow-600 hover:text-yellow-800"
                            onClick={() => handlePause(consumer.consumerId)}
                            disabled={actionLoading === consumer.consumerId}
                          >
                            <Pause className="h-3 w-3 mr-1" />
                            暂停
                          </Button>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>

          {/* 状态图例 */}
          <div className="p-3 border-t border-gray-100">
            <div className="flex items-center space-x-4 text-xs text-gray-600">
              <div className="flex items-center">
                <div className="w-2 h-2 rounded-full bg-green-500 mr-1" />
                <span>在线</span>
              </div>
              <div className="flex items-center">
                <div className="w-2 h-2 rounded-full bg-red-500 mr-1" />
                <span>离线</span>
              </div>
              <div className="flex items-center">
                <div className="w-2 h-2 rounded-full bg-yellow-500 mr-1" />
                <span>暂停</span>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
