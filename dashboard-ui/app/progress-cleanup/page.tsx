'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import Link from 'next/link';
import { DBMQAPIClient } from '@/lib/api';
import { StaleProgress, DetachedProgress } from '@/lib/types';
import { formatNumber, formatTimestamp } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  ArrowLeft,
  RefreshCw,
  Trash2,
  AlertTriangle,
  Activity,
  Unplug,
} from 'lucide-react';

// 默认 staleDays，与后端 dbmqapi.defaultStaleDays 保持一致
const DEFAULT_STALE_DAYS = 10;

type TabKey = 'stale' | 'detached';

export default function ProgressCleanupPage() {
  const [tab, setTab] = useState<TabKey>('stale');

  // Stale Tab 状态
  const [staleItems, setStaleItems] = useState<StaleProgress[]>([]);
  const [staleLoading, setStaleLoading] = useState(true);
  const [staleDays, setStaleDays] = useState<number>(DEFAULT_STALE_DAYS);
  const [pendingStaleDays, setPendingStaleDays] = useState<string>(String(DEFAULT_STALE_DAYS));
  const staleLoadingRef = useRef(false);

  // Detached Tab 状态
  const [detachedItems, setDetachedItems] = useState<DetachedProgress[]>([]);
  const [detachedLoading, setDetachedLoading] = useState(true);
  const detachedLoadingRef = useRef(false);

  const [error, setError] = useState<string | null>(null);
  const [deletingKey, setDeletingKey] = useState<string | null>(null);

  const loadStale = useCallback(async (days: number) => {
    if (staleLoadingRef.current) return;
    staleLoadingRef.current = true;
    try {
      setStaleLoading(true);
      const data = await DBMQAPIClient.getStaleProgress(days);
      setStaleItems(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load stale progress:', err);
      setError(err instanceof Error ? err.message : '加载消费滞后进度失败');
    } finally {
      setStaleLoading(false);
      staleLoadingRef.current = false;
    }
  }, []);

  const loadDetached = useCallback(async () => {
    if (detachedLoadingRef.current) return;
    detachedLoadingRef.current = true;
    try {
      setDetachedLoading(true);
      const data = await DBMQAPIClient.getDetachedProgress();
      setDetachedItems(data);
      setError(null);
    } catch (err) {
      console.error('Failed to load detached progress:', err);
      setError(err instanceof Error ? err.message : '加载孤立残留进度失败');
    } finally {
      setDetachedLoading(false);
      detachedLoadingRef.current = false;
    }
  }, []);

  useEffect(() => {
    if (tab === 'stale') {
      loadStale(staleDays);
    } else {
      loadDetached();
    }
  }, [tab, staleDays, loadStale, loadDetached]);

  const handleApplyStaleDays = () => {
    const parsed = parseInt(pendingStaleDays, 10);
    if (Number.isNaN(parsed) || parsed <= 0) {
      setError('staleDays 必须是大于 0 的整数');
      return;
    }
    setError(null);
    setStaleDays(parsed);
  };

  const handleRefresh = () => {
    if (tab === 'stale') {
      loadStale(staleDays);
    } else {
      loadDetached();
    }
  };

  const handleDeleteStale = async (item: StaleProgress) => {
    const key = `stale|${item.groupId}|${item.topic}|${item.partition}`;
    const confirmText =
      `确认删除消费严重滞后进度？\n` +
      `消费组: ${item.groupId}\n` +
      `Topic: ${item.topic}\n` +
      `分区: ${item.partition}\n` +
      `已消费消息时间: ${formatTimestamp(item.consumedMsgAt)}\n` +
      `最新消息时间: ${formatTimestamp(item.latestMsgAt)}\n` +
      `落后天数: ${item.staleDays}\n` +
      `落后条数: ${formatNumber(item.lagCount)}`;
    if (!confirm(confirmText)) return;

    setDeletingKey(key);
    try {
      await DBMQAPIClient.deleteStaleProgress(item.groupId, item.topic, item.partition, staleDays);
      await loadStale(staleDays);
    } catch (err) {
      console.error('Failed to delete stale progress:', err);
      setError(err instanceof Error ? err.message : '删除消费滞后进度失败');
    } finally {
      setDeletingKey(null);
    }
  };

  const handleDeleteDetached = async (item: DetachedProgress) => {
    const key = `detached|${item.groupId}|${item.topic}|${item.partition}`;
    const confirmText =
      `确认删除孤立残留进度？\n` +
      `消费组: ${item.groupId}（已不存在于 mq_consumer_group_generations）\n` +
      `Topic: ${item.topic}\n` +
      `分区: ${item.partition}\n` +
      `进度 generation: ${item.progressGenerationId}\n` +
      `残留天数: ${item.detachedDays}`;
    if (!confirm(confirmText)) return;

    setDeletingKey(key);
    try {
      await DBMQAPIClient.deleteDetachedProgress(item.groupId, item.topic, item.partition);
      await loadDetached();
    } catch (err) {
      console.error('Failed to delete detached progress:', err);
      setError(err instanceof Error ? err.message : '删除孤立残留进度失败');
    } finally {
      setDeletingKey(null);
    }
  };

  const totalLag = staleItems.reduce((sum, item) => sum + (item.lagCount || 0), 0);

  const loading = tab === 'stale' ? staleLoading : detachedLoading;

  return (
    <div className="min-h-screen bg-gray-50 text-sm">
      <header className="bg-white shadow-sm">
        <div className="page-shell">
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
                <h1 className="text-xl font-medium text-gray-900">进度清理</h1>
                <p className="text-sm text-gray-500 mt-0.5">
                  清理卡死消费进度（消费严重滞后）和孤立残留进度
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-3">
              <Button onClick={handleRefresh} size="sm" variant="outline" disabled={loading}>
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              </Button>
            </div>
          </div>
        </div>
      </header>

      <main className="page-shell py-4">
        <div className="mb-4 inline-flex rounded-md border border-gray-200 bg-white p-1 shadow-sm">
          <button
            type="button"
            className={`px-4 py-1.5 text-sm rounded ${
              tab === 'stale'
                ? 'bg-blue-600 text-white'
                : 'text-gray-600 hover:text-gray-900'
            }`}
            onClick={() => setTab('stale')}
          >
            <AlertTriangle className="inline h-3.5 w-3.5 mr-1" />
            消费严重滞后
          </button>
          <button
            type="button"
            className={`px-4 py-1.5 text-sm rounded ${
              tab === 'detached'
                ? 'bg-blue-600 text-white'
                : 'text-gray-600 hover:text-gray-900'
            }`}
            onClick={() => setTab('detached')}
          >
            <Unplug className="inline h-3.5 w-3.5 mr-1" />
            孤立残留
          </button>
        </div>

        {tab === 'stale' && (
          <Card className="mb-4 shadow-sm border-none bg-white">
            <CardContent className="py-3">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
                <div className="flex items-center gap-2">
                  <AlertTriangle className="h-4 w-4 text-orange-500" />
                  <div>
                    <div className="text-xs text-gray-500">滞后条目数</div>
                    <div className="font-semibold text-gray-900">{staleItems.length}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Activity className="h-4 w-4 text-blue-500" />
                  <div>
                    <div className="text-xs text-gray-500">当前 staleDays</div>
                    <div className="font-semibold text-gray-900">{staleDays}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Trash2 className="h-4 w-4 text-red-500" />
                  <div>
                    <div className="text-xs text-gray-500">滞后消息总数</div>
                    <div className="font-semibold text-gray-900">{formatNumber(totalLag)}</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {tab === 'detached' && (
          <Card className="mb-4 shadow-sm border-none bg-white">
            <CardContent className="py-3">
              <div className="flex items-center gap-2">
                <Unplug className="h-4 w-4 text-purple-500" />
                <div>
                  <div className="text-xs text-gray-500">孤立条目数</div>
                  <div className="font-semibold text-gray-900">{detachedItems.length}</div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}

        {tab === 'stale' && (
          <Card className="mb-4 shadow-sm border-none bg-white">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium">阈值设置</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap items-end gap-3">
                <div>
                  <label htmlFor="staleDays" className="mb-1 block text-xs font-medium text-gray-600">
                    staleDays (落后多少天视为严重滞后)
                  </label>
                  <input
                    id="staleDays"
                    type="number"
                    min={1}
                    value={pendingStaleDays}
                    onChange={(e) => setPendingStaleDays(e.target.value)}
                    className="w-32 rounded-md border border-gray-200 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  />
                </div>
                <Button
                  onClick={handleApplyStaleDays}
                  size="sm"
                  className="bg-blue-600 hover:bg-blue-700 text-white"
                >
                  应用
                </Button>
                <p className="text-xs text-gray-500">
                  已消费消息时间与 partition 最新消息时间相差至少这么多天才视为滞后，删除前后端会再次校验。
                </p>
              </div>
            </CardContent>
          </Card>
        )}

        {error && (
          <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
            {error}
          </div>
        )}

        {tab === 'stale' && (
          <Card className="shadow-sm border-none">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium flex items-center">
                <AlertTriangle className="h-4 w-4 mr-2 text-orange-500" />
                消费严重滞后进度列表
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">消费组</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Topic / 分区</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">已消费时间</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">最新消息时间</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">落后天数</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">落后条数</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">进度更新时间</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {staleLoading && staleItems.length === 0 ? (
                      <tr>
                        <td colSpan={8} className="px-3 py-4 text-center text-gray-500">加载中...</td>
                      </tr>
                    ) : staleItems.length === 0 ? (
                      <tr>
                        <td colSpan={8} className="px-3 py-4 text-center text-gray-500">
                          当前没有满足 staleDays={staleDays} 的滞后进度
                        </td>
                      </tr>
                    ) : (
                      staleItems.map((item) => {
                        const key = `stale|${item.groupId}|${item.topic}|${item.partition}`;
                        const isDeleting = deletingKey === key;
                        return (
                          <tr key={key} className="hover:bg-gray-50 transition-colors">
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 font-mono">{item.groupId}</td>
                            <td className="px-3 py-2 whitespace-nowrap">
                              <div className="text-sm text-gray-900">{item.topic}</div>
                              <div className="text-xs text-gray-500">分区 {item.partition}</div>
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600">
                              {item.consumedMsgAt ? formatTimestamp(item.consumedMsgAt) : '--'}
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600">
                              {item.latestMsgAt ? formatTimestamp(item.latestMsgAt) : '--'}
                            </td>
                            <td className="px-3 py-2 text-right">
                              <Badge className="bg-orange-100 text-orange-700 border-orange-200 font-medium text-xs">
                                {item.staleDays}
                              </Badge>
                            </td>
                            <td className="px-3 py-2 text-right">
                              <Badge
                                className={
                                  item.lagCount > 0
                                    ? 'bg-red-100 text-red-700 border-red-200 font-medium text-xs'
                                    : 'bg-gray-100 text-gray-600 border-gray-200 font-medium text-xs'
                                }
                              >
                                {formatNumber(item.lagCount)}
                              </Badge>
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600">
                              {item.progressUpdatedAt ? formatTimestamp(item.progressUpdatedAt) : '--'}
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap">
                              <Button
                                size="xs"
                                variant="outline"
                                className="text-red-600 hover:text-red-800"
                                disabled={isDeleting}
                                onClick={() => handleDeleteStale(item)}
                              >
                                <Trash2 className="h-3.5 w-3.5 mr-1" />
                                {isDeleting ? '删除中...' : '清理'}
                              </Button>
                            </td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {tab === 'detached' && (
          <Card className="shadow-sm border-none">
            <CardHeader className="pb-2">
              <CardTitle className="text-base font-medium flex items-center">
                <Unplug className="h-4 w-4 mr-2 text-purple-500" />
                孤立残留进度列表
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">消费组</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Topic / 分区</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">进度 Generation</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">进度更新时间</th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">残留天数</th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">操作</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100">
                    {detachedLoading && detachedItems.length === 0 ? (
                      <tr>
                        <td colSpan={6} className="px-3 py-4 text-center text-gray-500">加载中...</td>
                      </tr>
                    ) : detachedItems.length === 0 ? (
                      <tr>
                        <td colSpan={6} className="px-3 py-4 text-center text-gray-500">
                          当前没有孤立残留进度
                        </td>
                      </tr>
                    ) : (
                      detachedItems.map((item) => {
                        const key = `detached|${item.groupId}|${item.topic}|${item.partition}`;
                        const isDeleting = deletingKey === key;
                        return (
                          <tr key={key} className="hover:bg-gray-50 transition-colors">
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-900 font-mono">{item.groupId}</td>
                            <td className="px-3 py-2 whitespace-nowrap">
                              <div className="text-sm text-gray-900">{item.topic}</div>
                              <div className="text-xs text-gray-500">分区 {item.partition}</div>
                            </td>
                            <td className="px-3 py-2 text-right font-mono text-sm text-gray-700">
                              {item.progressGenerationId}
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-600">
                              {item.progressUpdatedAt ? formatTimestamp(item.progressUpdatedAt) : '--'}
                            </td>
                            <td className="px-3 py-2 text-right">
                              <Badge className="bg-purple-100 text-purple-700 border-purple-200 font-medium text-xs">
                                {item.detachedDays}
                              </Badge>
                            </td>
                            <td className="px-3 py-2 whitespace-nowrap">
                              <Button
                                size="xs"
                                variant="outline"
                                className="text-red-600 hover:text-red-800"
                                disabled={isDeleting}
                                onClick={() => handleDeleteDetached(item)}
                              >
                                <Trash2 className="h-3.5 w-3.5 mr-1" />
                                {isDeleting ? '删除中...' : '清理'}
                              </Button>
                            </td>
                          </tr>
                        );
                      })
                    )}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}
      </main>
    </div>
  );
}
