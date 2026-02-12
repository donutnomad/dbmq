'use client';

import { useState, useEffect } from 'react';
import { DashboardData, TopicMetrics, ConsumerGroupMetrics, DBMQStats } from '@/lib/types';
import { DBMQAPIClient, getAccessToken, clearAccessToken } from '@/lib/api';
import { formatNumber, formatUptime, formatBytes } from '@/lib/utils';
import { StatCard } from '@/components/ui/card';
import { StatusBadge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Database,
  Users,
  MessageSquare,
  Clock,
  RefreshCw,
  Plus,
  Settings,
  Settings2,
  Send,
  Activity,
  Zap,
  Server,
  HardDrive,
  Radio,
  LogOut,
} from 'lucide-react';
import Link from 'next/link';

export function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [dbmqStats, setDbmqStats] = useState<DBMQStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  // 加载仪表板数据
  const loadData = async () => {
    try {
      const [dashboardData, stats] = await Promise.all([
        DBMQAPIClient.getDashboardData(),
        DBMQAPIClient.getDBMQStats().catch(() => null),
      ]);
      setData(dashboardData);
      setDbmqStats(stats);
      setError(null);
      setLastUpdate(new Date());
    } catch (err) {
      console.error('Failed to load dashboard data:', err);
      setError(err instanceof Error ? err.message : '加载数据失败');
    } finally {
      setLoading(false);
    }
  };

  // 初始化和自动刷新
  useEffect(() => {
    loadData();
    
    let interval: NodeJS.Timeout;
    if (autoRefresh) {
      interval = setInterval(loadData, 5000);
    }

    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [autoRefresh]);

  // 计算统计数据
  const totalMessages = data?.topics?.reduce((sum, topic) => sum + (topic.messageCount || 0), 0) || 0;

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载仪表板数据中...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
            <p className="font-bold">错误</p>
            <p>{error}</p>
          </div>
          <Button onClick={loadData} variant="primary">
            重试
          </Button>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* 头部 - 使用更现代的设计 */}
      <header className="bg-white shadow-sm">
        <div className="max-w-[98%] mx-auto px-4">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <h1 className="text-xl font-medium text-gray-900">DBMQ 监控仪表板</h1>
              <StatusBadge status="online" className="px-3 py-1">
                系统运行中
              </StatusBadge>
            </div>
            <div className="flex items-center space-x-4">
              <Link href="/clusters">
                <Button size="sm" variant="outline" className="text-blue-600 hover:text-blue-800">
                  <Server className="h-4 w-4 mr-1" />
                  集群管理
                </Button>
              </Link>
              <Link href="/consumers">
                <Button size="sm" variant="outline" className="text-teal-600 hover:text-teal-800">
                  <Radio className="h-4 w-4 mr-1" />
                  消费者管理
                </Button>
              </Link>
              <div className="flex items-center">
                <label className="flex items-center cursor-pointer">
                  <input
                    type="checkbox"
                    checked={autoRefresh}
                    onChange={(e) => setAutoRefresh(e.target.checked)}
                    className="sr-only"
                  />
                  <div className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${
                    autoRefresh ? 'bg-blue-600' : 'bg-gray-200'
                  }`}>
                    <span className={`inline-block h-4 w-4 transform rounded-full bg-white transition-transform ${
                      autoRefresh ? 'translate-x-6' : 'translate-x-1'
                    }`} />
                  </div>
                  <span className="ml-2 text-sm text-gray-600">自动刷新</span>
                </label>
              </div>
              {lastUpdate && (
                <div className="text-xs text-gray-500">
                  最后更新: {lastUpdate.toLocaleTimeString('zh-CN')}
                </div>
              )}
              <Button onClick={loadData} size="sm" variant="outline" disabled={loading}>
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              </Button>
              {getAccessToken() && (
                <Button
                  onClick={() => { clearAccessToken(); window.location.reload(); }}
                  size="sm"
                  variant="outline"
                  className="text-red-500 hover:text-red-700"
                  title="退出登录"
                >
                  <LogOut className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-[98%] mx-auto px-4 py-4">
        {/* 统计卡片 - 更紧凑的布局 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-4">
          <StatCard
            title="Topic 总数"
            value={data?.topics?.length || 0}
            icon={<Database className="h-5 w-5 text-blue-500" />}
            className="bg-blue-50 border-none"
          />
          <StatCard
            title="消费组总数"
            value={data?.consumerGroups?.length || 0}
            icon={<Users className="h-5 w-5 text-green-500" />}
            className="bg-green-50 border-none"
          />
          <StatCard
            title="分区总数"
            value={dbmqStats?.cluster?.partitionCount || 0}
            icon={<HardDrive className="h-5 w-5 text-cyan-500" />}
            className="bg-cyan-50 border-none"
          />
          <StatCard
            title="总消息数"
            value={formatNumber(totalMessages)}
            icon={<MessageSquare className="h-5 w-5 text-purple-500" />}
            className="bg-purple-50 border-none"
          />
          <StatCard
            title="系统运行时间"
            value={formatUptime(data?.system?.uptime)}
            icon={<Clock className="h-5 w-5 text-orange-500" />}
            className="bg-orange-50 border-none"
          />
          <StatCard
            title="服务状态"
            value={<StatusBadge status="online" className="px-2 py-0.5">运行中</StatusBadge>}
            icon={<Zap className="h-5 w-5 text-indigo-500" />}
            className="bg-indigo-50 border-none"
          />
        </div>

        {/* DBMQ 系统统计面板 */}
        {dbmqStats && (
          <div className="bg-white rounded-lg shadow-sm mb-4">
            <div className="p-4 border-b flex justify-between items-center">
              <h2 className="text-base font-medium flex items-center">
                <Activity className="h-4 w-4 mr-2 text-purple-500" />
                系统详细统计
              </h2>
              <Link href="/clusters">
                <Button size="sm" variant="outline" className="text-gray-600 hover:text-gray-800">
                  <Server className="h-4 w-4 mr-1" />
                  查看集群详情
                </Button>
              </Link>
            </div>
            <div className="p-4">
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">Broker ID</div>
                  <div className="text-lg font-semibold text-gray-900">{dbmqStats.broker.brokerId}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">主机地址</div>
                  <div className="text-sm font-semibold text-gray-900">{dbmqStats.broker.host}:{dbmqStats.broker.port}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">系统版本</div>
                  <div className="text-lg font-semibold text-gray-900">{dbmqStats.system.version}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">分区数量</div>
                  <div className="text-lg font-semibold text-gray-900">{dbmqStats.cluster.partitionCount}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">存储大小</div>
                  <div className="text-lg font-semibold text-gray-900">{formatBytes(dbmqStats.cluster.totalSizeBytes)}</div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">Broker 运行时间</div>
                  <div className="text-sm font-semibold text-gray-900">{dbmqStats.broker.uptime}</div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* 内容区域 - 更现代的设计 */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <TopicsList topics={data?.topics || []} />
          <ConsumerGroupsList consumerGroups={data?.consumerGroups || []} />
        </div>
      </main>
    </div>
  );
}

// Topics 列表组件 - 现代化设计
function TopicsList({ topics }: { topics: TopicMetrics[] }) {
  return (
    <div className="bg-white rounded-lg shadow-sm">
      <div className="p-4 flex justify-between items-center">
        <h2 className="text-base font-medium flex items-center">
          <Database className="h-4 w-4 mr-2 text-blue-500" />
          Topic 列表
        </h2>
        <Link href="/topics/create">
          <Button size="sm" variant="outline" className="text-green-600 hover:text-green-800">
            <Plus className="h-4 w-4 mr-1" />
            创建Topic
          </Button>
        </Link>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-gray-50">
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Topic 名称
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                分区数
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                消息数
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                状态
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {topics.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-4 text-center text-gray-500">
                  暂无 Topic 数据
                </td>
              </tr>
            ) : (
              topics.map((topic) => {
                const topicName = topic.name || topic.topicName || '--';
                return (
                  <tr key={topicName} className="hover:bg-gray-50 transition-colors group">
                    <td className="px-4 py-2 whitespace-nowrap">
                      <Link
                        href={`/topics/?name=${encodeURIComponent(topicName)}`}
                        className="block w-full h-full"
                      >
                        <span className="text-sm text-gray-900 group-hover:text-gray-700">
                          {topicName}
                        </span>
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap text-xs text-gray-600">
                      <Link
                        href={`/topics/?name=${encodeURIComponent(topicName)}`}
                        className="block w-full h-full"
                      >
                        {topic.partitionCount || topic.partitions?.length || '--'}
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap text-xs text-gray-600">
                      <Link
                        href={`/topics/?name=${encodeURIComponent(topicName)}`}
                        className="block w-full h-full"
                      >
                        {formatNumber(topic.messageCount || 0)}
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap">
                      <Link
                        href={`/topics/?name=${encodeURIComponent(topicName)}`}
                        className="block w-full h-full"
                      >
                        <StatusBadge status={topic.status || 'active'} />
                      </Link>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// 消费组列表组件 - 现代化设计
function ConsumerGroupsList({ consumerGroups }: { consumerGroups: ConsumerGroupMetrics[] }) {
  return (
    <div className="bg-white rounded-lg shadow-sm">
      <div className="p-4 flex justify-between items-center">
        <h2 className="text-base font-medium flex items-center">
          <Users className="h-4 w-4 mr-2 text-blue-500" />
          消费组列表
        </h2>
        <Link href="/manual-assignments">
          <Button size="sm" variant="outline" className="text-purple-600 hover:text-purple-800">
            <Settings2 className="h-4 w-4 mr-1" />
            手动分配
          </Button>
        </Link>
        <Link href="/producer">
          <Button size="sm" variant="outline" className="text-blue-600 hover:text-blue-800">
            <Send className="h-4 w-4 mr-1" />
            发送消息
          </Button>
        </Link>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead>
            <tr className="bg-gray-50">
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                消费组 ID
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                状态
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                成员数
              </th>
              <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                延迟
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {consumerGroups.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-4 text-center text-gray-500">
                  暂无消费组数据
                </td>
              </tr>
            ) : (
              consumerGroups.map((group) => {
                const groupId = group.groupId || group.name || '--';
                return (
                  <tr key={groupId} className="hover:bg-gray-50 transition-colors group">
                    <td className="px-4 py-2 whitespace-nowrap">
                      <Link
                        href={`/consumer-groups/?id=${encodeURIComponent(groupId)}`}
                        className="block w-full h-full"
                      >
                        <span className="text-sm text-gray-900 group-hover:text-gray-700">
                          {groupId}
                        </span>
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap">
                      <Link
                        href={`/consumer-groups/?id=${encodeURIComponent(groupId)}`}
                        className="block w-full h-full"
                      >
                        <StatusBadge status={group.state || group.status || 'unknown'} />
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap text-xs text-gray-600">
                      <Link
                        href={`/consumer-groups/?id=${encodeURIComponent(groupId)}`}
                        className="block w-full h-full"
                      >
                        {group.memberCount || group.members?.length || 0}
                      </Link>
                    </td>
                    <td className="px-4 py-2 whitespace-nowrap text-xs text-gray-600">
                      <Link
                        href={`/consumer-groups/?id=${encodeURIComponent(groupId)}`}
                        className="block w-full h-full"
                      >
                        {formatNumber(group.lag || 0)}
                      </Link>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
} 