'use client';

import { useState, useEffect, useCallback } from 'react';
import { ClusterInfo, ClusterMetricsDetail, BrokerInfo, DBMQStats } from '@/lib/types';
import { DBMQAPIClient } from '@/lib/api';
import { formatNumber, formatBytes, formatUptime } from '@/lib/utils';
import { StatCard } from '@/components/ui/card';
import { StatusBadge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import {
  Server,
  Database,
  Users,
  MessageSquare,
  HardDrive,
  Clock,
  RefreshCw,
  ArrowLeft,
  Activity,
  Cpu,
  Network,
  CheckCircle,
  XCircle,
} from 'lucide-react';
import Link from 'next/link';

export default function ClustersPage() {
  const [clusters, setClusters] = useState<ClusterInfo[]>([]);
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null);
  const [clusterMetrics, setClusterMetrics] = useState<ClusterMetricsDetail | null>(null);
  const [brokers, setBrokers] = useState<BrokerInfo[]>([]);
  const [dbmqStats, setDbmqStats] = useState<DBMQStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);

  // 加载集群列表
  const loadClusters = useCallback(async () => {
    try {
      const clusterList = await DBMQAPIClient.getClusters();
      setClusters(clusterList);

      // 自动选择第一个集群
      if (clusterList.length > 0 && !selectedCluster) {
        setSelectedCluster(clusterList[0].clusterId);
      }

      setError(null);
    } catch (err) {
      console.error('Failed to load clusters:', err);
      setError(err instanceof Error ? err.message : '加载集群列表失败');
    }
  }, [selectedCluster]);

  // 加载集群详情
  const loadClusterDetails = useCallback(async () => {
    if (!selectedCluster) return;

    try {
      const [metrics, brokerList] = await Promise.all([
        DBMQAPIClient.getClusterMetrics(selectedCluster),
        DBMQAPIClient.getClusterBrokers(selectedCluster),
      ]);

      setClusterMetrics(metrics);
      setBrokers(brokerList);
    } catch (err) {
      console.error('Failed to load cluster details:', err);
    }
  }, [selectedCluster]);

  // 加载 DBMQ 统计
  const loadDBMQStats = useCallback(async () => {
    try {
      const stats = await DBMQAPIClient.getDBMQStats();
      setDbmqStats(stats);
    } catch (err) {
      console.error('Failed to load DBMQ stats:', err);
    }
  }, []);

  // 加载所有数据
  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      await Promise.all([
        loadClusters(),
        loadDBMQStats(),
      ]);
      setLastUpdate(new Date());
    } finally {
      setLoading(false);
    }
  }, [loadClusters, loadDBMQStats]);

  // 初始化
  useEffect(() => {
    loadData();
  }, [loadData]);

  // 加载集群详情（当选中的集群变化时）
  useEffect(() => {
    if (selectedCluster) {
      loadClusterDetails();
    }
  }, [selectedCluster, loadClusterDetails]);

  // 自动刷新
  useEffect(() => {
    if (!autoRefresh) return;

    const interval = setInterval(() => {
      loadData();
      if (selectedCluster) {
        loadClusterDetails();
      }
    }, 10000);

    return () => clearInterval(interval);
  }, [autoRefresh, loadData, loadClusterDetails, selectedCluster]);

  if (loading && !clusters.length) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载集群数据中...</p>
        </div>
      </div>
    );
  }

  if (error && !clusters.length) {
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

  const currentCluster = clusters.find(c => c.clusterId === selectedCluster);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* 头部 */}
      <header className="bg-white shadow-sm">
        <div className="page-shell">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-4">
              <Link href="/" className="text-gray-500 hover:text-gray-700">
                <ArrowLeft className="h-5 w-5" />
              </Link>
              <h1 className="text-xl font-medium text-gray-900 flex items-center">
                <Server className="h-5 w-5 mr-2 text-blue-500" />
                集群管理
              </h1>
              {currentCluster && (
                <StatusBadge
                  status={currentCluster.status === 'online' ? 'active' : 'error'}
                  className="px-3 py-1"
                >
                  {currentCluster.status === 'online' ? '在线' : '离线'}
                </StatusBadge>
              )}
            </div>
            <div className="flex items-center space-x-4">
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
            </div>
          </div>
        </div>
      </header>

      <main className="page-shell py-4">
        {/* 集群概览统计 */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-4">
          <StatCard
            title="集群数"
            value={clusters.length}
            icon={<Server className="h-5 w-5 text-blue-500" />}
            className="bg-blue-50 border-none"
          />
          <StatCard
            title="Topic 总数"
            value={clusterMetrics?.topicCount || dbmqStats?.cluster?.topicCount || 0}
            icon={<Database className="h-5 w-5 text-green-500" />}
            className="bg-green-50 border-none"
          />
          <StatCard
            title="分区总数"
            value={clusterMetrics?.partitionCount || dbmqStats?.cluster?.partitionCount || 0}
            icon={<HardDrive className="h-5 w-5 text-purple-500" />}
            className="bg-purple-50 border-none"
          />
          <StatCard
            title="消费组总数"
            value={clusterMetrics?.consumerGroupCount || dbmqStats?.cluster?.consumerGroupCount || 0}
            icon={<Users className="h-5 w-5 text-orange-500" />}
            className="bg-orange-50 border-none"
          />
          <StatCard
            title="总消息数"
            value={formatNumber(clusterMetrics?.totalMessages || dbmqStats?.cluster?.totalMessages || 0)}
            icon={<MessageSquare className="h-5 w-5 text-rose-500" />}
            className="bg-rose-50 border-none"
          />
          <StatCard
            title="系统运行时间"
            value={formatUptime(dbmqStats?.system?.uptime)}
            icon={<Clock className="h-5 w-5 text-indigo-500" />}
            className="bg-indigo-50 border-none"
          />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
          {/* 集群列表 */}
          <div className="bg-white rounded-lg shadow-sm">
            <div className="p-4 border-b">
              <h2 className="text-base font-medium flex items-center">
                <Server className="h-4 w-4 mr-2 text-blue-500" />
                集群列表
              </h2>
            </div>
            <div className="p-4">
              {clusters.length === 0 ? (
                <p className="text-gray-500 text-center py-4">暂无集群数据</p>
              ) : (
                <div className="space-y-2">
                  {clusters.map((cluster) => (
                    <div
                      key={cluster.clusterId}
                      onClick={() => setSelectedCluster(cluster.clusterId)}
                      className={`p-3 rounded-lg cursor-pointer transition-colors ${
                        selectedCluster === cluster.clusterId
                          ? 'bg-blue-50 border border-blue-200'
                          : 'bg-gray-50 hover:bg-gray-100 border border-transparent'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3">
                          {cluster.status === 'online' ? (
                            <CheckCircle className="h-5 w-5 text-green-500" />
                          ) : (
                            <XCircle className="h-5 w-5 text-red-500" />
                          )}
                          <div>
                            <div className="font-medium text-gray-900">{cluster.name}</div>
                            <div className="text-xs text-gray-500">{cluster.clusterId}</div>
                          </div>
                        </div>
                        <div className="text-right">
                          <div className="text-sm text-gray-600">{cluster.brokerCount} Broker</div>
                          <StatusBadge
                            status={cluster.status === 'online' ? 'active' : 'error'}
                            className="text-xs"
                          >
                            {cluster.status === 'online' ? '在线' : '离线'}
                          </StatusBadge>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Broker 列表 */}
          <div className="lg:col-span-2 bg-white rounded-lg shadow-sm">
            <div className="p-4 border-b">
              <h2 className="text-base font-medium flex items-center">
                <Cpu className="h-4 w-4 mr-2 text-green-500" />
                Broker 列表
                {currentCluster && (
                  <span className="ml-2 text-sm text-gray-500">
                    - {currentCluster.name}
                  </span>
                )}
              </h2>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Broker ID
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      主机
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      端口
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      版本
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      运行时间
                    </th>
                    <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      状态
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {brokers.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-4 py-4 text-center text-gray-500">
                        {selectedCluster ? '暂无 Broker 数据' : '请选择一个集群'}
                      </td>
                    </tr>
                  ) : (
                    brokers.map((broker) => (
                      <tr key={broker.brokerId} className="hover:bg-gray-50 transition-colors">
                        <td className="px-4 py-3 whitespace-nowrap">
                          <div className="flex items-center">
                            <Cpu className="h-4 w-4 mr-2 text-gray-400" />
                            <span className="text-sm font-medium text-gray-900">
                              {broker.brokerId}
                            </span>
                          </div>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <div className="flex items-center">
                            <Network className="h-4 w-4 mr-2 text-gray-400" />
                            <span className="text-sm text-gray-600">{broker.host}</span>
                          </div>
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600">
                          {broker.port}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600">
                          {broker.version}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap text-sm text-gray-600">
                          {broker.uptime}
                        </td>
                        <td className="px-4 py-3 whitespace-nowrap">
                          <StatusBadge status="active">
                            在线
                          </StatusBadge>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>

        {/* DBMQ 详细统计 */}
        {dbmqStats && (
          <div className="mt-4 bg-white rounded-lg shadow-sm">
            <div className="p-4 border-b">
              <h2 className="text-base font-medium flex items-center">
                <Activity className="h-4 w-4 mr-2 text-purple-500" />
                DBMQ 系统统计
              </h2>
            </div>
            <div className="p-4">
              <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">Topic 数量</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {dbmqStats.cluster.topicCount}
                  </div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">分区数量</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {dbmqStats.cluster.partitionCount}
                  </div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">消费组数量</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {dbmqStats.cluster.consumerGroupCount}
                  </div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">总消息数</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {formatNumber(dbmqStats.cluster.totalMessages)}
                  </div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">存储大小</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {formatBytes(dbmqStats.cluster.totalSizeBytes)}
                  </div>
                </div>
                <div className="p-3 bg-gray-50 rounded-lg">
                  <div className="text-xs text-gray-500 mb-1">系统版本</div>
                  <div className="text-lg font-semibold text-gray-900">
                    {dbmqStats.system.version}
                  </div>
                </div>
              </div>

              {/* Broker 信息卡片 */}
              <div className="mt-4 p-4 bg-blue-50 rounded-lg">
                <h3 className="text-sm font-medium text-blue-900 mb-3">当前 Broker 信息</h3>
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                  <div>
                    <div className="text-xs text-blue-600">Broker ID</div>
                    <div className="text-sm font-medium text-blue-900">{dbmqStats.broker.brokerId}</div>
                  </div>
                  <div>
                    <div className="text-xs text-blue-600">主机</div>
                    <div className="text-sm font-medium text-blue-900">{dbmqStats.broker.host}</div>
                  </div>
                  <div>
                    <div className="text-xs text-blue-600">端口</div>
                    <div className="text-sm font-medium text-blue-900">{dbmqStats.broker.port}</div>
                  </div>
                  <div>
                    <div className="text-xs text-blue-600">版本</div>
                    <div className="text-sm font-medium text-blue-900">{dbmqStats.broker.version}</div>
                  </div>
                  <div>
                    <div className="text-xs text-blue-600">运行时间</div>
                    <div className="text-sm font-medium text-blue-900">{dbmqStats.broker.uptime}</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
