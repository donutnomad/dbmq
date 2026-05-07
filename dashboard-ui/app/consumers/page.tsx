'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { ArrowLeft, RefreshCw, Search, UserSquare2, Wifi, WifiOff, Clock3 } from 'lucide-react';
import { DBMQAPIClient } from '@/lib/api';
import { Consumer } from '@/lib/types';
import { formatTimestamp } from '@/lib/utils';
import { Button } from '@/components/ui/button';

function statusLabel(status?: string): string {
  switch (status) {
    case 'online':
      return '在线';
    case 'offline':
      return '离线';
    case 'timeout':
      return '超时';
    default:
      return status || '未知';
  }
}

function StatusIcon({ status }: { status?: string }) {
  if (status === 'online') {
    return <Wifi className="h-4 w-4 text-blue-600" />;
  }
  if (status === 'timeout') {
    return <Clock3 className="h-4 w-4 text-amber-600" />;
  }
  return <WifiOff className="h-4 w-4 text-gray-500" />;
}

function assignmentSummary(assignment?: Record<string, number[]>): string {
  if (!assignment || Object.keys(assignment).length === 0) {
    return '--';
  }

  return Object.entries(assignment)
    .map(([topic, partitions]) => `${topic} (${partitions.length})`)
    .join(', ');
}

export default function ConsumersPage() {
  const [consumers, setConsumers] = useState<Consumer[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [groupKeyword, setGroupKeyword] = useState('');
  const [consumerKeyword, setConsumerKeyword] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'online' | 'timeout' | 'offline'>('all');

  const loadConsumers = useCallback(async () => {
    try {
      setLoading(true);
      const data = await DBMQAPIClient.getConsumers();
      setConsumers(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load consumers:', err);
      setError(err instanceof Error ? err.message : '加载消费者失败');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadConsumers();
  }, [loadConsumers]);

  const filteredConsumers = useMemo(() => {
    return consumers.filter((consumer) => {
      const matchGroup = !groupKeyword || consumer.groupId.toLowerCase().includes(groupKeyword.toLowerCase());
      const matchConsumer = !consumerKeyword || consumer.memberId.toLowerCase().includes(consumerKeyword.toLowerCase());
      const matchStatus = statusFilter === 'all' || consumer.status === statusFilter;
      return matchGroup && matchConsumer && matchStatus;
    });
  }, [consumers, groupKeyword, consumerKeyword, statusFilter]);

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm">
        <div className="page-shell">
          <div className="flex items-center justify-between py-4">
            <div className="flex items-center gap-4">
              <Link href="/" className="text-gray-500 hover:text-gray-700">
                <ArrowLeft className="h-5 w-5" />
              </Link>
              <h1 className="flex items-center text-xl font-medium text-gray-900">
                <UserSquare2 className="mr-2 h-5 w-5 text-indigo-500" />
                消费者
              </h1>
            </div>
            <Button onClick={loadConsumers} size="sm" variant="outline" disabled={loading}>
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            </Button>
          </div>
        </div>
      </header>

      <main className="page-shell py-4">
        <div className="mb-4 grid gap-3 md:grid-cols-3">
          <label className="flex items-center gap-2 rounded-lg border border-gray-200 bg-white px-3 py-2">
            <Search className="h-4 w-4 text-gray-400" />
            <input
              value={groupKeyword}
              onChange={(e) => setGroupKeyword(e.target.value)}
              placeholder="筛选消费组"
              className="w-full bg-transparent text-sm text-gray-900 outline-none"
            />
          </label>
          <label className="flex items-center gap-2 rounded-lg border border-gray-200 bg-white px-3 py-2">
            <Search className="h-4 w-4 text-gray-400" />
            <input
              value={consumerKeyword}
              onChange={(e) => setConsumerKeyword(e.target.value)}
              placeholder="筛选消费者 ID"
              className="w-full bg-transparent text-sm text-gray-900 outline-none"
            />
          </label>
          <select
            value={statusFilter}
            onChange={(e) => setStatusFilter(e.target.value as 'all' | 'online' | 'timeout' | 'offline')}
            className="rounded-lg border border-gray-200 bg-white px-3 py-2 text-sm text-gray-900 outline-none"
          >
            <option value="all">全部状态</option>
            <option value="online">在线</option>
            <option value="timeout">超时</option>
            <option value="offline">离线</option>
          </select>
        </div>

        <div className="rounded-lg bg-white shadow-sm">
          <div className="flex items-center justify-between p-4">
            <div className="text-sm text-gray-600">
              共 {filteredConsumers.length} 个消费者
            </div>
          </div>

          {loading && consumers.length === 0 ? (
            <div className="flex items-center justify-center px-6 py-16 text-gray-500">
              <RefreshCw className="mr-2 h-5 w-5 animate-spin" />
              加载消费者中...
            </div>
          ) : error ? (
            <div className="px-6 py-10 text-sm text-red-600">{error}</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1120px]">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">状态</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">消费者 ID</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">消费组</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">代际</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">最后心跳</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">订阅 Topic</th>
                    <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">分配概览</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {filteredConsumers.length === 0 ? (
                    <tr>
                      <td colSpan={7} className="px-4 py-10 text-center text-sm text-gray-500">
                        暂无消费者数据
                      </td>
                    </tr>
                  ) : (
                    filteredConsumers.map((consumer) => (
                      <tr key={`${consumer.groupId}:${consumer.memberId}`} className="hover:bg-gray-50">
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-700">
                          <div className="flex items-center gap-2">
                            <StatusIcon status={consumer.status} />
                            <span>{statusLabel(consumer.status)}</span>
                          </div>
                        </td>
                        <td className="px-4 py-3 align-top text-sm text-gray-900">
                          <div className="max-w-[320px] break-all">{consumer.memberId}</div>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm">
                          <Link
                            href={`/consumer-groups/?id=${encodeURIComponent(consumer.groupId)}`}
                            className="text-blue-600 hover:text-blue-800"
                          >
                            {consumer.groupId}
                          </Link>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600">
                          {consumer.generationId ?? '--'}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600">
                          {consumer.lastHeartbeat ? formatTimestamp(consumer.lastHeartbeat) : '--'}
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-600">
                          <div className="max-w-[260px] break-all">
                            {consumer.subscribedTopics?.length ? consumer.subscribedTopics.join(', ') : '--'}
                          </div>
                        </td>
                        <td className="px-4 py-3 text-sm text-gray-600">
                          <div className="max-w-[360px] break-all">
                            {assignmentSummary(consumer.assignment)}
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </main>
    </div>
  );
}
