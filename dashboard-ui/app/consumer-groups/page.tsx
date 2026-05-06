'use client';

import React, { useState, useEffect, useCallback, useRef, Suspense } from 'react';
import {useRouter, useSearchParams} from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { ConsumerGroupMetrics, PartitionLag, ManualAssignment, CreateManualAssignmentRequest, TopicMetrics } from '@/lib/types';
import { formatNumber, formatTimestamp, formatBytes } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import {
  ArrowLeft,
  RefreshCw,
  Users,
  Clock,
  BarChart,
  LayoutGrid,
  Activity,
  Hash,
  Plus,
  Trash2,
  Zap,
  Wifi,
  WifiOff,
} from 'lucide-react';
import Link from 'next/link';

function ConsumerGroupDetailContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const groupId = searchParams.get('id') as string;

  const [group, setGroup] = useState<ConsumerGroupMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showMemberDetails, setShowMemberDetails] = useState<Record<string, boolean>>({});
  const [manualAssignments, setManualAssignments] = useState<ManualAssignment[]>([]);
  const [topics, setTopics] = useState<TopicMetrics[]>([]);
  const [showAssignmentForm, setShowAssignmentForm] = useState(false);
  const [assignmentLoading, setAssignmentLoading] = useState(false);
  const [manualAssignmentError, setManualAssignmentError] = useState<string | null>(null);
  const [rebalancing, setRebalancing] = useState(false);
  const [deletingGroup, setDeletingGroup] = useState(false);
  const [consumerPatternOptions, setConsumerPatternOptions] = useState<string[]>([]);
  const [formData, setFormData] = useState<CreateManualAssignmentRequest>({
    group_id: groupId || '',
    consumer_id_pattern: '',
    topic: '',
    partition: 0,
  });
  const [autoRefresh, setAutoRefresh] = useState(true); // 自动刷新开关
  const [refreshInterval, setRefreshInterval] = useState(5000); // 刷新间隔（毫秒）
  const loadFunctionRef = useRef<(() => Promise<void>) | null>(null);

  // 派生状态：计算总分区数
  const totalAssignedPartitions = group?.members?.reduce(
      (sum, member) => {
        if (!member.assignment || typeof member.assignment !== 'object') return sum;
        // assignment现在是对象格式 {"topic": [partitions...]}
        return sum + Object.values(member.assignment).reduce((total: number, partitions: any) => {
          return total + (Array.isArray(partitions) ? partitions.length : 0);
        }, 0);
      },
      0
  ) ?? 0;

  // 加载消费组详情（完整加载，显示 loading 状态）
  const loadConsumerGroupDetail = useCallback(async () => {
    try {
      setLoading(true);
      const [groupData, assignmentData, topicData] = await Promise.all([
        DBMQAPIClient.getConsumerGroup(groupId),
        DBMQAPIClient.getManualAssignments(groupId),
        DBMQAPIClient.getTopics(true),
      ]);
      setGroup(groupData);
      setManualAssignments(assignmentData);
      setTopics(topicData);
      setManualAssignmentError(null);
      setError(null);
    } catch (err) {
      console.error('Failed to load consumer group detail:', err);
      setError(err instanceof Error ? err.message : '加载消费组详情失败');
    } finally {
      setLoading(false);
    }
  }, [groupId]);

  // 静默刷新数据（不显示 loading 状态，用于自动刷新）
  const refreshDataSilently = useCallback(async () => {
    try {
      const [groupData, assignmentData] = await Promise.all([
        DBMQAPIClient.getConsumerGroup(groupId),
        DBMQAPIClient.getManualAssignments(groupId),
      ]);
      setGroup(groupData);
      setManualAssignments(assignmentData);
      setManualAssignmentError(null);
      setError(null);
    } catch (err) {
      console.error('Failed to refresh consumer group data:', err);
    }
  }, [groupId]);

  // 更新 ref 以保持最新的刷新函数
  useEffect(() => {
    loadFunctionRef.current = refreshDataSilently;
  }, [refreshDataSilently]);

  // 获取状态徽章类型
  const getStatusVariant = (status: string | undefined): 'default' | 'success' | 'warning' | 'error' => {
    if (!status) return 'default';

    const statusMap: Record<string, 'default' | 'success' | 'warning' | 'error'> = {
      'Active': 'success',
      'Stable': 'success',
      'Empty': 'default',
      'Dead': 'error'
    };

    return statusMap[status] || 'default';
  };

  // 获取状态显示文本
  const getStatusText = (status: string | undefined): string => {
    if (!status) return '未知';

    const statusMap: Record<string, string> = {
      'Active': '活跃',
      'Stable': '稳定',
      'Empty': '空闲',
      'Dead': '停止'
    };

    return statusMap[status] || status;
  };

  // 切换成员详情显示
  const toggleMemberDetails = (memberId: string) => {
    setShowMemberDetails(prev => ({
      ...prev,
      [memberId]: !prev[memberId]
    }));
  };

  const getPartitionCount = (): number => {
    const topic = topics.find(t => (t.name || t.topicName) === formData.topic);
    return topic?.partitionCount || topic?.partitions?.length || 1;
  };

  const groupTopicNames = Array.from(
    new Set((group?.partitionLags || []).map(lag => lag.topic).filter((topic): topic is string => Boolean(topic)))
  );

  const buildConsumerPatternOptions = (consumerId: string): string[] => {
    const parts = consumerId.split(':').filter(Boolean);
    if (parts.length === 0) {
      return [];
    }

    return Array.from(new Set([
      `${parts[0]}:*`,
      parts.length > 1 ? `${parts.slice(0, -1).join(':')}:*` : consumerId,
      consumerId,
    ]));
  };

  const openAssignmentForm = (consumerId?: string) => {
    const assignedTopic = groupTopicNames[0] || '';
    const patternOptions = consumerId
      ? buildConsumerPatternOptions(consumerId)
      : Array.from(
          new Set((group?.members || []).flatMap(member => buildConsumerPatternOptions(member.memberId || '')))
        );
    setConsumerPatternOptions(patternOptions);
    setFormData({
      group_id: groupId || '',
      consumer_id_pattern: patternOptions[0] || consumerId || '',
      topic: assignedTopic,
      partition: 0,
    });
    setShowAssignmentForm(true);
    setManualAssignmentError(null);
  };

  const loadManualAssignments = useCallback(async () => {
    const assignmentData = await DBMQAPIClient.getManualAssignments(groupId);
    setManualAssignments(assignmentData);
    setManualAssignmentError(null);
  }, [groupId]);

  const handleAssignmentSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.group_id || !formData.consumer_id_pattern.trim() || !formData.topic) {
      setManualAssignmentError('请填写消费者ID模式和 Topic');
      return;
    }

    setAssignmentLoading(true);
    try {
      await DBMQAPIClient.createManualAssignment(formData);
      setShowAssignmentForm(false);
      setFormData({
        group_id: groupId || '',
        consumer_id_pattern: '',
        topic: '',
        partition: 0,
      });
      setConsumerPatternOptions([]);
      await loadManualAssignments();
    } catch (err) {
      console.error('Failed to create manual assignment:', err);
      setManualAssignmentError(err instanceof Error ? err.message : '创建分配规则失败');
    } finally {
      setAssignmentLoading(false);
    }
  };

  const handleDeleteAssignment = async (id: number) => {
    if (!confirm('确定要删除这条分配规则吗？')) return;

    try {
      await DBMQAPIClient.deleteManualAssignment(id);
      await loadManualAssignments();
    } catch (err) {
      console.error('Failed to delete manual assignment:', err);
      setManualAssignmentError(err instanceof Error ? err.message : '删除分配规则失败');
    }
  };

  const handleTriggerRebalance = async () => {
    setRebalancing(true);
    try {
      await DBMQAPIClient.triggerRebalance(groupId);
      setManualAssignmentError(null);
      setTimeout(() => {
        refreshDataSilently();
      }, 3000);
    } catch (err) {
      console.error('Failed to trigger rebalance:', err);
      setManualAssignmentError(err instanceof Error ? err.message : '触发重新均衡失败');
    } finally {
      setRebalancing(false);
    }
  };

  const handleDeleteGroup = async () => {
    if (!canDeleteGroup || deletingGroup) return;
    if (!confirm(`确定要删除消费组 ${decodeURIComponent(groupId)} 吗？相关心跳、消费进度和手动分配规则会一起删除。`)) return;

    setDeletingGroup(true);
    try {
      await DBMQAPIClient.deleteConsumerGroup(groupId);
      router.push('/');
    } catch (err) {
      console.error('Failed to delete consumer group:', err);
      setManualAssignmentError(err instanceof Error ? err.message : '删除消费组失败');
    } finally {
      setDeletingGroup(false);
    }
  };

  // 获取延迟严重程度
  const getLagSeverity = (lag: number | undefined): string => {
    if (!lag) return 'bg-green-100 text-green-800';
    if (lag > 1000) return 'bg-red-100 text-red-800';
    if (lag > 100) return 'bg-yellow-100 text-yellow-800';
    return 'bg-green-100 text-green-800';
  };

  // 判断消费者是否在线
  const isConsumerOnline = (lastHeartbeat: string | number | Date | undefined): boolean => {
    if (!lastHeartbeat) return false;
    const heartbeatTime = new Date(lastHeartbeat).getTime();
    const now = Date.now();
    const heartbeatTimeout = 30000; // 30秒超时
    return (now - heartbeatTime) < heartbeatTimeout;
  };

  const onlineMemberCount = (group?.members || []).filter(member => isConsumerOnline(member.lastHeartbeat)).length;
  const canDeleteGroup = Boolean(groupId) && onlineMemberCount === 0;

  // 获取消费者状态的行样式
  const getConsumerRowStyle = (lastHeartbeat: string | number | Date | undefined): string => {
    return isConsumerOnline(lastHeartbeat)
        ? 'hover:bg-gray-50 transition-colors'
        : 'bg-red-50 hover:bg-red-100 transition-colors';
  };

  // 计算消费进度百分比
  const calculateProgressPercentage = (lag: PartitionLag): number => {
    const total = lag.totalMessageCount || 0;
    const pending = lag.lag || 0;
    if (total <= 0) return 100;
    return ((total - pending) / total) * 100;
  };

  useEffect(() => {
    if (groupId) {
      loadConsumerGroupDetail();
    }
  }, [groupId, loadConsumerGroupDetail]);

  // 自动刷新逻辑
  useEffect(() => {
    if (!autoRefresh || !groupId) return;

    const timer = setInterval(() => {
      if (loadFunctionRef.current) {
        loadFunctionRef.current();
      }
    }, refreshInterval);

    return () => clearInterval(timer);
  }, [autoRefresh, refreshInterval, groupId]);

  if (loading) {
    return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
            <p className="text-gray-600">加载消费组详情中...</p>
          </div>
        </div>
    );
  }

  if (error && !group) {
    return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
              <p className="font-bold">错误</p>
              <p>{error}</p>
            </div>
            <Button onClick={loadConsumerGroupDetail} variant="primary">
              重试
            </Button>
          </div>
        </div>
    );
  }

  return (
      <div className="min-h-screen bg-gray-50 text-sm">
        {/* 头部 - 使用更现代的设计 */}
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
                  <h1 className="text-xl font-medium text-gray-900">消费组详情</h1>
                  <p className="text-sm text-gray-500 mt-0.5">{decodeURIComponent(groupId)}</p>
                </div>
              </div>
              <div className="flex items-center space-x-3">
                <Badge variant={getStatusVariant(group?.state || group?.status)} className="px-3 py-1">
                  {getStatusText(group?.state || group?.status)}
                </Badge>

                <Button
                  onClick={handleDeleteGroup}
                  size="sm"
                  variant={canDeleteGroup ? "destructive" : "outline"}
                  disabled={!canDeleteGroup || deletingGroup}
                  title={canDeleteGroup ? '删除消费组' : '当前消费组存在在线成员'}
                >
                  <Trash2 className="h-4 w-4 mr-1" />
                  {deletingGroup ? '删除中...' : '删除消费组'}
                </Button>

                {/* 自动刷新间隔选择 */}
                <select
                  value={refreshInterval}
                  onChange={(e) => setRefreshInterval(Number(e.target.value))}
                  className="px-2 py-1 text-xs border border-gray-200 rounded-md focus:ring-blue-500 focus:border-blue-500"
                  disabled={!autoRefresh}
                >
                  <option value={3000}>3秒</option>
                  <option value={5000}>5秒</option>
                  <option value={10000}>10秒</option>
                  <option value={30000}>30秒</option>
                </select>

                {/* 自动刷新开关 */}
                <Button
                  onClick={() => setAutoRefresh(!autoRefresh)}
                  size="sm"
                  variant={autoRefresh ? "default" : "outline"}
                  className={autoRefresh ? "bg-green-600 hover:bg-green-700 text-white" : ""}
                >
                  {autoRefresh ? (
                    <>
                      <Activity className="h-4 w-4 mr-1" />
                      自动刷新
                    </>
                  ) : (
                    <>
                      <Activity className="h-4 w-4 mr-1" />
                      已暂停
                    </>
                  )}
                </Button>

                {/* 手动刷新按钮 */}
                <Button onClick={loadConsumerGroupDetail} size="sm" variant="outline" disabled={loading}>
                  <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                </Button>
              </div>
            </div>
          </div>
        </header>

        <main className="page-shell py-4">
          <Card className="mb-4 shadow-sm border-none bg-white">
            <CardContent className="py-3">
              <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
                <div className="flex items-center gap-2">
                  <Users className="h-4 w-4 text-blue-500" />
                  <div>
                    <div className="text-xs text-gray-500">成员数</div>
                    <div className="font-semibold text-gray-900">{group?.memberCount || group?.members?.length || 0}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Clock className="h-4 w-4 text-orange-500" />
                  <div>
                    <div className="text-xs text-gray-500">待消费</div>
                    <div className="font-semibold text-gray-900">{formatNumber(group?.totalLag || group?.lag || 0)}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <Hash className="h-4 w-4 text-purple-500" />
                  <div>
                    <div className="text-xs text-gray-500">代际ID</div>
                    <div className="font-semibold text-gray-900">{group?.generationId || '--'}</div>
                  </div>
                </div>
                <div className="flex items-center gap-2">
                  <LayoutGrid className="h-4 w-4 text-green-500" />
                  <div>
                    <div className="text-xs text-gray-500">分区数</div>
                    <div className="font-semibold text-gray-900">{totalAssignedPartitions}</div>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* 消费进度概览 - 更现代的设计 */}
          {group?.partitionLags && group.partitionLags.length > 0 && (
              <Card className="mb-4 shadow-sm border-none bg-white">
                <CardHeader className="pb-2">
                  <CardTitle className="text-base font-medium flex items-center">
                    <Activity className="h-4 w-4 mr-2 text-blue-500" />
                    消费进度概览
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {groupTopicNames.map((topic) => {
                      const topicPartitions = group.partitionLags?.filter(lag => lag.topic === topic) || [];
                      const avgProgress = topicPartitions.length > 0
                          ? topicPartitions.reduce((sum, lag) => sum + calculateProgressPercentage(lag), 0) / topicPartitions.length
                          : 0;
                      const totalLag = topicPartitions.reduce((sum, lag) => sum + (lag.lag || 0), 0);

                      return (
                          <div key={topic} className="bg-gray-50 rounded-lg p-3">
                            <div className="flex justify-between items-center mb-2">
                              <div>
                                <span className="text-xs font-medium text-gray-700 block">{topic}</span>
                                <span className="text-xs text-gray-500">延迟: {totalLag.toLocaleString()}</span>
                              </div>
                              <span className="text-xs font-medium text-gray-700">{Math.round(avgProgress)}%</span>
                            </div>
                            <div className="w-full bg-gray-200 rounded-full h-1.5">
                              <div
                                  className={`h-1.5 rounded-full transition-all ${
                                      avgProgress > 90 ? 'bg-green-500' :
                                          avgProgress > 60 ? 'bg-blue-500' :
                                              avgProgress > 30 ? 'bg-yellow-500' : 'bg-red-500'
                                  }`}
                                  style={{ width: `${avgProgress}%` }}
                              ></div>
                            </div>
                          </div>
                      );
                    })}
                  </div>
                </CardContent>
              </Card>
          )}

          <div>
            {/* 消费者成员 - 现代化表格设计 */}
            <Card className="shadow-sm border-none">
                <CardHeader className="pb-2">
                  <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
                    <CardTitle className="text-base font-medium flex items-center">
                      <Users className="h-4 w-4 mr-2 text-blue-500" />
                      消费者成员
                    </CardTitle>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => openAssignmentForm()}
                        className="text-green-700 hover:text-green-800"
                      >
                        <Plus className="h-4 w-4 mr-1" />
                        添加规则
                      </Button>
                      <Button
                        size="sm"
                        disabled={rebalancing}
                        onClick={handleTriggerRebalance}
                        className="bg-blue-600 hover:bg-blue-700 text-white"
                      >
                        <Zap className={`h-4 w-4 mr-1 ${rebalancing ? 'animate-pulse' : ''}`} />
                        {rebalancing ? '应用中...' : '立即应用更改'}
                      </Button>
                    </div>
                  </div>
                </CardHeader>
                <CardContent>
                  {manualAssignmentError && (
                    <div className="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                      {manualAssignmentError}
                    </div>
                  )}

                  {showAssignmentForm && (
                    <form onSubmit={handleAssignmentSubmit} className="mb-4 rounded-md border border-gray-100 bg-gray-50 p-3">
                      <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
                        <div>
                          <label htmlFor="consumer_id_pattern" className="mb-1 block text-xs font-medium text-gray-600">
                            消费者ID模式
                          </label>
                          <select
                            id="consumer_id_pattern"
                            required
                            value={formData.consumer_id_pattern}
                            onChange={(e) => setFormData(prev => ({ ...prev, consumer_id_pattern: e.target.value }))}
                            className="w-full rounded-md border border-gray-200 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"
                          >
                            <option value="">选择消费者ID模式</option>
                            {consumerPatternOptions.map((pattern) => (
                              <option key={pattern} value={pattern}>
                                {pattern}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <label htmlFor="topic" className="mb-1 block text-xs font-medium text-gray-600">
                            Topic
                          </label>
                          <select
                            id="topic"
                            required
                            value={formData.topic}
                            onChange={(e) => setFormData(prev => ({ ...prev, topic: e.target.value, partition: 0 }))}
                            className="w-full rounded-md border border-gray-200 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"
                          >
                            <option value="">选择 Topic</option>
                            {groupTopicNames.map((topicName) => (
                              <option key={topicName} value={topicName}>
                                {topicName}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div>
                          <label htmlFor="partition" className="mb-1 block text-xs font-medium text-gray-600">
                            分区
                          </label>
                          <select
                            id="partition"
                            value={formData.partition}
                            onChange={(e) => setFormData(prev => ({ ...prev, partition: parseInt(e.target.value) || 0 }))}
                            className="w-full rounded-md border border-gray-200 px-3 py-2 text-sm shadow-sm focus:border-blue-500 focus:ring-blue-500"
                            disabled={!formData.topic}
                          >
                            {Array.from({ length: getPartitionCount() }, (_, i) => (
                              <option key={i} value={i}>
                                分区 {i}
                              </option>
                            ))}
                          </select>
                        </div>
                        <div className="flex items-end gap-2">
                          <Button type="submit" disabled={assignmentLoading} className="bg-green-600 hover:bg-green-700 text-white">
                            {assignmentLoading ? '创建中...' : '创建规则'}
                          </Button>
                          <Button type="button" variant="outline" onClick={() => setShowAssignmentForm(false)}>
                            取消
                          </Button>
                        </div>
                      </div>
                    </form>
                  )}

                  <div className="mb-4 rounded-md border border-blue-100 bg-blue-50 p-3 text-xs text-blue-700">
                    消费者ID模式支持精确匹配和通配符匹配。创建或删除规则后，点击“立即应用更改”触发当前消费组重新均衡。
                  </div>

                  <div className="overflow-x-auto">
                    <table className="w-full">
                      <thead>
                      <tr className="bg-gray-50">
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          消费者ID / 状态
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          最后心跳
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          分区数
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          详情
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          分配
                        </th>
                      </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                      {!group?.members || group.members.length === 0 ? (
                          <tr>
                            <td colSpan={5} className="px-3 py-4 text-center text-gray-500">
                              暂无成员数据
                            </td>
                          </tr>
                      ) : (
                          group.members.map((member, index) => (
                              <React.Fragment key={member.memberId || index}>
                                <tr className={getConsumerRowStyle(member.lastHeartbeat)}>
                                  <td className="px-3 py-2 whitespace-nowrap">
                                    <div className="flex items-center">
                                      {isConsumerOnline(member.lastHeartbeat) ? (
                                        <Wifi className="h-4 w-4 mr-2 text-green-600" />
                                      ) : (
                                        <WifiOff className="h-4 w-4 mr-2 text-red-600" />
                                      )}
                                      <span className="text-gray-900">{member.memberId || '--'}</span>
                                      {!isConsumerOnline(member.lastHeartbeat) && (
                                          <Badge variant="error" className="ml-2 text-xs bg-red-100 text-red-700 border-red-200">
                                            离线
                                          </Badge>
                                      )}
                                    </div>
                                  </td>
                                  <td className="px-3 py-2 whitespace-nowrap">
                                <span className="text-gray-600">
                                  {member.lastHeartbeat ? formatTimestamp(member.lastHeartbeat) : '--'}
                                </span>
                                  </td>
                                  <td className="px-3 py-2 whitespace-nowrap">
                                    <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
                                      {(() => {
                                        if (!member.assignment || typeof member.assignment !== 'object') return 0;
                                        return Object.values(member.assignment).reduce((total: number, partitions: any) => {
                                          return total + (Array.isArray(partitions) ? partitions.length : 0);
                                        }, 0);
                                      })()}
                                    </Badge>
                                  </td>
                                  <td className="px-3 py-2 whitespace-nowrap">
                                    <Button
                                        variant="outline"
                                        size="xs"
                                        onClick={() => toggleMemberDetails(member.memberId)}
                                        className="text-blue-600 hover:text-blue-800"
                                    >
                                      {showMemberDetails[member.memberId] ? '隐藏' : '查看'}
                                    </Button>
                                  </td>
                                  <td className="px-3 py-2 whitespace-nowrap">
                                    <Button
                                      variant="outline"
                                      size="xs"
                                      onClick={() => openAssignmentForm(member.memberId)}
                                      className="text-green-700 hover:text-green-800"
                                    >
                                      分配
                                    </Button>
                                  </td>
                                </tr>
                                {showMemberDetails[member.memberId] && (
                                    <tr>
                                      <td colSpan={5} className="px-3 py-2 bg-gray-50">
                                        <div className="space-y-3">
                                          {/* 订阅主题 */}
                                          <div>
                                            <h4 className="text-xs font-semibold text-gray-700 mb-1">订阅主题</h4>
                                            <div className="flex flex-wrap gap-1">
                                              {member.subscribedTopics && member.subscribedTopics.length > 0 ? (
                                                  member.subscribedTopics.map((topic, i) => (
                                                      <Badge key={i} variant="outline" className="bg-green-50 text-green-700 border-green-200">
                                                        {topic}
                                                      </Badge>
                                                  ))
                                              ) : (
                                                  <span className="text-xs text-gray-500">无订阅主题</span>
                                              )}
                                            </div>
                                          </div>

                                          {/* 分区分配 */}
                                          <div>
                                            <h4 className="text-xs font-semibold text-gray-700 mb-1">分区分配</h4>
                                            {member.assignment && typeof member.assignment === 'object' && Object.keys(member.assignment).length > 0 ? (
                                                <div className="space-y-2">
                                                  {Object.entries(member.assignment).map(([topic, partitions]) => (
                                                      <div key={topic} className="bg-white rounded shadow-sm p-2">
                                                        <div className="flex items-center justify-between">
                                                          <Badge variant="outline" className="bg-blue-50 text-blue-700 border-blue-200">
                                                            {topic}
                                                          </Badge>
                                                          <span className="text-xs text-gray-500">
                                                  分区: {Array.isArray(partitions) ? partitions.join(', ') : '无'}
                                                </span>
                                                        </div>
                                                      </div>
                                                  ))}
                                                </div>
                                            ) : (
                                                <span className="text-xs text-gray-500">无分区分配</span>
                                            )}
                                          </div>
                                        </div>
                                      </td>
                                    </tr>
                                )}
                              </React.Fragment>
                          ))
                      )}
                      </tbody>
                    </table>
                  </div>
                  {/* 状态图例 */}
                  <div className="mt-3 pt-3 border-t border-gray-100">
                    <div className="flex items-center space-x-4 text-xs text-gray-600">
                      <div className="flex items-center">
                        <Wifi className="h-3.5 w-3.5 mr-1 text-green-600" />
                        <span>在线</span>
                      </div>
                      <div className="flex items-center">
                        <WifiOff className="h-3.5 w-3.5 mr-1 text-red-600" />
                        <span>离线 (超过30秒未发送心跳)</span>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 overflow-x-auto border-t border-gray-100 pt-4">
                    <div className="mb-2 flex items-center justify-between">
                      <h3 className="text-sm font-medium text-gray-800">手动分配规则</h3>
                      <Badge variant="outline" className="bg-gray-50 text-gray-700 border-gray-200">
                        {manualAssignments.length} 条
                      </Badge>
                    </div>
                    <table className="w-full">
                      <thead>
                        <tr className="bg-gray-50">
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            消费者ID模式
                          </th>
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            Topic
                          </th>
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            分区
                          </th>
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            创建时间
                          </th>
                          <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                            操作
                          </th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-gray-100">
                        {manualAssignments.length === 0 ? (
                          <tr>
                            <td colSpan={5} className="px-3 py-4 text-center text-gray-500">
                              暂无手动分配规则，当前使用自动分区分配策略
                            </td>
                          </tr>
                        ) : (
                          manualAssignments.map((assignment) => (
                            <tr key={assignment.id} className="hover:bg-gray-50 transition-colors">
                              <td className="px-3 py-2 whitespace-nowrap font-mono text-sm text-gray-900">
                                {assignment.consumer_id_pattern}
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-700">
                                {assignment.topic}
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-700">
                                {assignment.partition}
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap text-sm text-gray-500">
                                {new Date(assignment.created_at).toLocaleString('zh-CN')}
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap">
                                <Button
                                  size="xs"
                                  variant="outline"
                                  className="text-red-600 hover:text-red-800"
                                  onClick={() => handleDeleteAssignment(assignment.id)}
                                >
                                  <Trash2 className="h-3.5 w-3.5" />
                                </Button>
                              </td>
                            </tr>
                          ))
                        )}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
          </div>

          {/* 分区延迟详情 - 现代化表格设计 */}
          <div className="mt-4">
            <Card className="shadow-sm border-none">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium flex items-center">
                  <BarChart className="h-4 w-4 mr-2 text-blue-500" />
                  分区延迟详情
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                    <tr className="bg-gray-50">
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Topic/分区
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                        最新Offset
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                        当前Offset
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider" title="未消费的实际消息条数（非offset差值，因消息清理后ID不连续）">
                        待消费
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[200px]">
                        消费进度
                      </th>
                      <th className="px-3 py-2 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                        消息数/大小
                      </th>
                    </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                    {!group?.partitionLags || group.partitionLags.length === 0 ? (
                        <tr>
                          <td colSpan={6} className="px-3 py-4 text-center text-gray-500">
                            暂无延迟数据
                          </td>
                        </tr>
                    ) : (
                        group.partitionLags.map((lag, index) => {
                          const latest = lag.latestOffset || 0;
                          const current = lag.currentOffset || 0;
                          const watermark = lag.initialTopicWatermark ?? 0;
                          const total = lag.totalMessageCount || 0;
                          const pending = lag.lag || 0;
                          // 基于实际消息数计算进度: (总消息数 - 待消费数) / 总消息数
                          let progressPct = total > 0 ? ((total - pending) / total) * 100 : 0;
                          // 水位在进度条中的位置百分比
                          const watermarkPct = latest > 0 ? Math.min(100, (watermark / latest) * 100) : 0;
                          return (
                              <tr key={index} className="hover:bg-gray-50 transition-colors">
                                <td className="px-3 py-2">
                                  <Link
                                      href={`/topics?name=${encodeURIComponent(lag.topic || '')}`}
                                      className="text-blue-600 hover:text-blue-800 font-medium text-sm"
                                  >
                                    {lag.topic || '--'}
                                  </Link>
                                  <div className="text-xs text-gray-500">分区 {lag.partition ?? '--'}</div>
                                </td>
                                <td className="px-3 py-2 text-right font-mono text-sm text-gray-700">
                                  {latest.toLocaleString()}
                                </td>
                                <td className="px-3 py-2 text-right font-mono text-sm text-blue-600 font-medium">
                                  {current.toLocaleString()}
                                </td>
                                <td className="px-3 py-2 text-right">
                                  <div title="未消费的实际消息条数">
                                    <Badge className={`${getLagSeverity(lag.lag)} font-medium text-xs`}>
                                      {(lag.lag || 0).toLocaleString()} 条
                                    </Badge>
                                  </div>
                                  {latest - current !== (lag.lag || 0) && latest - current > 0 && (
                                      <div className="text-[10px] text-gray-400 mt-0.5" title="Offset差值，因消息清理后ID不连续，与实际待消费数不同">
                                        offset差: {(latest - current).toLocaleString()}
                                      </div>
                                  )}
                                </td>
                                <td className="px-3 py-2">
                                  <div className="space-y-1">
                                    <div className="flex items-center gap-2">
                                      <div className="flex-1 relative">
                                        <div className="w-full bg-gray-200 rounded-full h-2 overflow-hidden">
                                          {/* 已消费部分 */}
                                          <div
                                              className="h-full bg-blue-500 rounded-full"
                                              style={{ width: `${progressPct}%` }}
                                          />
                                        </div>
                                        {/* 初始水位标记线 */}
                                        {watermark > 0 && watermarkPct > 0.5 && watermarkPct < 99.5 && (
                                            <div
                                                className="absolute top-[-2px] w-0.5 h-3 bg-orange-500"
                                                style={{ left: `${watermarkPct}%` }}
                                                title={`初始水位: ${watermark.toLocaleString()}`}
                                            />
                                        )}
                                      </div>
                                      <span className="text-xs font-medium text-gray-700 w-14 text-right">{progressPct.toFixed(4)}%</span>
                                    </div>
                                    {watermark > 0 && (
                                        <div className="flex items-center gap-1 text-[10px] text-orange-600">
                                          <div className="w-2 h-0.5 bg-orange-500 rounded" />
                                          <span>水位: {watermark.toLocaleString()}</span>
                                        </div>
                                    )}
                                  </div>
                                </td>
                                <td className="px-3 py-2 text-right text-xs text-gray-700">
                                  <div>{(lag.totalMessageCount || 0).toLocaleString()} 条</div>
                                  {lag.partitionSizeBytes !== undefined && (
                                      <div className="text-gray-500">{formatBytes(lag.partitionSizeBytes)}</div>
                                  )}
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
          </div>
        </main>
      </div>
  );
}

function LoadingFallback() {
  return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载中...</p>
        </div>
      </div>
  );
}

export default function ConsumerGroupDetailPage() {
  return (
      <Suspense fallback={<LoadingFallback />}>
        <ConsumerGroupDetailContent />
      </Suspense>
  );
}
