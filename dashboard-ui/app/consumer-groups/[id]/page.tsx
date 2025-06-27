'use client';

import { useState, useEffect, useCallback } from 'react';
import { useParams } from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { ConsumerGroupMetrics, PartitionLag } from '@/lib/types';
import { formatNumber, formatTimestamp, formatBytes } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle, StatCard } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { 
  ArrowLeft, 
  RefreshCw, 
  Users, 
  Clock, 
  AlertCircle,
  Database,
  Calendar,
  Zap,
  BarChart,
  CheckCircle,
  XCircle,
  LayoutGrid,
  Activity,
  Settings,
  Server,
  Hash,
} from 'lucide-react';
import Link from 'next/link';

export default function ConsumerGroupDetailPage() {
  const params = useParams();
  const groupId = params.id as string;

  const [group, setGroup] = useState<ConsumerGroupMetrics | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showMemberDetails, setShowMemberDetails] = useState<Record<string, boolean>>({});

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

  // 加载消费组详情
  const loadConsumerGroupDetail = useCallback(async () => {
    try {
      setLoading(true);
      const groupData = await DBMQAPIClient.getConsumerGroup(groupId);
      setGroup(groupData);
      setError(null);
    } catch (err) {
      console.error('Failed to load consumer group detail:', err);
      setError(err instanceof Error ? err.message : '加载消费组详情失败');
    } finally {
      setLoading(false);
    }
  }, [groupId]);

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

  // 获取消费者状态的行样式
  const getConsumerRowStyle = (lastHeartbeat: string | number | Date | undefined): string => {
    return isConsumerOnline(lastHeartbeat) 
      ? 'hover:bg-gray-50 transition-colors' 
      : 'bg-red-50 hover:bg-red-100 transition-colors';
  };

  // 计算消费进度百分比
  const calculateProgressPercentage = (lag: PartitionLag): number => {
    if (!lag.latestOffset || lag.latestOffset === 0) return 100;
    if (!lag.currentOffset && lag.currentOffset !== 0) return 0;
    
    const total = lag.latestOffset;
    const consumed = lag.currentOffset;
    return Math.min(100, Math.max(0, Math.round((consumed / total) * 100)));
  };

  useEffect(() => {
    if (groupId) {
      loadConsumerGroupDetail();
    }
  }, [groupId, loadConsumerGroupDetail]);

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
                <h1 className="text-xl font-medium text-gray-900">消费组详情</h1>
                <p className="text-sm text-gray-500 mt-0.5">{decodeURIComponent(groupId)}</p>
              </div>
            </div>
            <div className="flex items-center space-x-3">
              <Badge variant={getStatusVariant(group?.state || group?.status)} className="px-3 py-1">
                {getStatusText(group?.state || group?.status)}
              </Badge>
              <Button onClick={loadConsumerGroupDetail} size="sm" variant="outline" disabled={loading}>
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              </Button>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-[98%] mx-auto px-4 py-4">
        {/* 统计卡片 - 更紧凑的布局 */}
        <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3 mb-4">
          <StatCard
            title="成员数"
            value={group?.memberCount || group?.members?.length || 0}
            icon={<Users className="h-5 w-5 text-blue-500" />}
            className="bg-blue-50 border-none"
          />
          <StatCard
            title="总延迟"
            value={formatNumber(group?.totalLag || group?.lag || 0)}
            icon={<Clock className="h-5 w-5 text-orange-500" />}
            className="bg-orange-50 border-none"
          />
          <StatCard
            title="代际ID"
            value={group?.generationId || '--'}
            icon={<Hash className="h-5 w-5 text-purple-500" />}
            className="bg-purple-50 border-none"
          />
          <StatCard
            title="分区数"
            value={totalAssignedPartitions}
            icon={<LayoutGrid className="h-5 w-5 text-green-500" />}
            className="bg-green-50 border-none"
          />
          <StatCard
            title="协调器"
            value={group?.coordinator || '--'}
            icon={<Server className="h-5 w-5 text-indigo-500" />}
            className="bg-indigo-50 border-none"
          />
          <StatCard
            title="分配策略"
            value={group?.assignmentStrategy || '轮询'}
            icon={<Settings className="h-5 w-5 text-rose-500" />}
            className="bg-rose-50 border-none"
          />
        </div>

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
                {group.assignedTopics?.map((topic) => {
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
                          <span className="text-xs text-gray-500">延迟: {formatNumber(totalLag)}</span>
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

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-4">
          {/* 消费者成员 - 现代化表格设计 */}
          <div className="lg:col-span-3">
            <Card className="shadow-sm border-none">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium flex items-center">
                  <Users className="h-4 w-4 mr-2 text-blue-500" />
                  消费者成员
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <table className="w-full">
                    <thead>
                      <tr className="bg-gray-50">
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          消费者ID / 状态
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          客户端ID
                        </th>
                        <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          主机
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
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                      {!group?.members || group.members.length === 0 ? (
                        <tr>
                          <td colSpan={6} className="px-3 py-4 text-center text-gray-500">
                            暂无成员数据
                          </td>
                        </tr>
                      ) : (
                        group.members.map((member, index) => (
                          <>
                            <tr key={member.memberId || index} className={getConsumerRowStyle(member.lastHeartbeat)}>
                              <td className="px-3 py-2 whitespace-nowrap">
                                <div className="flex items-center">
                                  <div className={`w-2 h-2 rounded-full mr-2 ${
                                    isConsumerOnline(member.lastHeartbeat) 
                                      ? 'bg-green-500' 
                                      : 'bg-red-500'
                                  }`}></div>
                                  <span className="text-gray-900">{member.memberId || '--'}</span>
                                  {!isConsumerOnline(member.lastHeartbeat) && (
                                    <Badge variant="error" className="ml-2 text-xs bg-red-100 text-red-700 border-red-200">
                                      离线
                                    </Badge>
                                  )}
                                </div>
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap text-gray-600">
                                {member.clientId || '--'}
                              </td>
                              <td className="px-3 py-2 whitespace-nowrap text-gray-600">
                                {member.host || '--'}
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
                            </tr>
                            {showMemberDetails[member.memberId] && (
                              <tr>
                                <td colSpan={6} className="px-3 py-2 bg-gray-50">
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
                          </>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
                {/* 状态图例 */}
                <div className="mt-3 pt-3 border-t border-gray-100">
                  <div className="flex items-center space-x-4 text-xs text-gray-600">
                    <div className="flex items-center">
                      <div className="w-2 h-2 rounded-full bg-green-500 mr-1"></div>
                      <span>在线</span>
                    </div>
                    <div className="flex items-center">
                      <div className="w-2 h-2 rounded-full bg-red-500 mr-1"></div>
                      <span>离线 (超过30秒未发送心跳)</span>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* 消费组信息 - 更紧凑和现代的设计 */}
          <div className="lg:col-span-1">
            <Card className="shadow-sm border-none">
              <CardHeader className="pb-2">
                <CardTitle className="text-base font-medium flex items-center">
                  <Database className="h-4 w-4 mr-2 text-blue-500" />
                  消费组信息
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-gray-50 rounded-lg p-3">
                    <h3 className="text-xs font-medium text-gray-700 mb-2">时间信息</h3>
                    <div className="space-y-2">
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-gray-600">最后活动</span>
                        <span className="text-xs font-medium">
                          {group?.lastActivity ? formatTimestamp(group.lastActivity) : '--'}
                        </span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-gray-600">创建时间</span>
                        <span className="text-xs font-medium">
                          {group?.createdAt ? formatTimestamp(group.createdAt) : '--'}
                        </span>
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-50 rounded-lg p-3">
                    <h3 className="text-xs font-medium text-gray-700 mb-2">分配的Topics</h3>
                    {!group?.assignedTopics || group.assignedTopics.length === 0 ? (
                      <p className="text-xs text-gray-500">暂无分配的Topics</p>
                    ) : (
                      <div className="flex flex-wrap gap-1">
                        {group.assignedTopics.map((topic, index) => (
                          <Link href={`/topics/${encodeURIComponent(topic)}`} key={index}>
                            <Badge 
                              variant="outline" 
                              className="bg-blue-50 text-blue-700 border-blue-200 cursor-pointer hover:bg-blue-100"
                            >
                              <Database className="h-3 w-3 mr-1" />
                              {topic}
                            </Badge>
                          </Link>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
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
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        ID范围
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        消费进度
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        消息统计
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        延迟状态
                      </th>
                      <th className="px-3 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        大小/更新时间
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
                        const progressPercentage = lag.consumedPercentage || calculateProgressPercentage(lag);
                        return (
                          <tr key={index} className="hover:bg-gray-50 transition-colors">
                            {/* Topic/分区 */}
                            <td className="px-3 py-2">
                              <div>
                                <Link 
                                  href={`/topics/${encodeURIComponent(lag.topic || '')}`}
                                  className="text-blue-600 hover:text-blue-800 font-medium text-sm"
                                >
                                  {lag.topic || '--'}
                                </Link>
                                <div className="text-xs text-gray-500">
                                  分区 {lag.partition !== undefined ? lag.partition : '--'}
                                </div>
                              </div>
                            </td>
                            
                            {/* ID范围 */}
                            <td className="px-3 py-2">
                              <div className="text-xs space-y-1">
                                <div>
                                  <span className="text-gray-500">首个:</span>
                                  <span className="ml-1 font-mono text-gray-700">
                                    {lag.firstMessageId !== undefined && lag.firstMessageId !== -1 
                                      ? formatNumber(lag.firstMessageId) 
                                      : '无'}
                                  </span>
                                </div>
                                <div>
                                  <span className="text-gray-500">最新:</span>
                                  <span className="ml-1 font-mono text-gray-700">
                                    {lag.lastMessageId !== undefined && lag.lastMessageId !== -1 
                                      ? formatNumber(lag.lastMessageId) 
                                      : '无'}
                                  </span>
                                </div>
                                <div>
                                  <span className="text-gray-500">当前:</span>
                                  <span className="ml-1 font-mono text-blue-600 font-medium">
                                    {formatNumber(lag.currentOffset || 0)}
                                  </span>
                                </div>
                                {lag.initialTopicWatermark !== undefined && lag.initialTopicWatermark !== null && (
                                  <div>
                                    <span className="text-gray-500">初始水位:</span>
                                    <span className="ml-1 font-mono text-orange-600 font-medium" title="消费组首次加入topic时的topic最新消息ID">
                                      {formatNumber(lag.initialTopicWatermark)}
                                    </span>
                                  </div>
                                )}
                              </div>
                            </td>
                            
                            {/* 消费进度 */}
                            <td className="px-3 py-2">
                              <div className="space-y-2">
                                <div className="flex items-center justify-between text-xs">
                                  <span className="text-gray-500">进度</span>
                                  <span className="font-medium">{progressPercentage.toFixed(1)}%</span>
                                </div>
                                <Progress value={progressPercentage} className="h-1.5" />
                                <div className="flex justify-between text-xs text-gray-500">
                                  <span>已消费: {formatNumber(lag.consumedMessages || 0)}</span>
                                  <span>剩余: {formatNumber(lag.remainingMessages || 0)}</span>
                                </div>
                              </div>
                            </td>
                            
                            {/* 消息统计 */}
                            <td className="px-3 py-2">
                              <div className="text-xs space-y-1">
                                <div>
                                  <span className="text-gray-500">总数:</span>
                                  <span className="ml-1 font-medium text-gray-700">
                                    {formatNumber(lag.totalMessageCount || 0)}
                                  </span>
                                </div>
                                {lag.partitionSizeBytes !== undefined && (
                                  <div>
                                    <span className="text-gray-500">大小:</span>
                                    <span className="ml-1 font-medium text-gray-700">
                                      {formatBytes(lag.partitionSizeBytes)}
                                    </span>
                                  </div>
                                )}
                              </div>
                            </td>
                            
                            {/* 延迟状态 */}
                            <td className="px-3 py-2">
                              <div className="space-y-1">
                                <Badge className={`${getLagSeverity(lag.lag)} font-medium text-xs`}>
                                  延迟 {formatNumber(lag.lag || 0)}
                                </Badge>
                                {lag.metadata && (
                                  <div className="text-xs text-gray-400" title={lag.metadata}>
                                    {lag.metadata}
                                  </div>
                                )}
                                <div className="text-xs text-gray-500">
                                  代际: {lag.generationId || '--'}
                                </div>
                              </div>
                            </td>
                            
                            {/* 大小/更新时间 */}
                            <td className="px-3 py-2">
                              <div className="text-xs space-y-1">
                                {lag.updatedAt && (
                                  <div>
                                    <span className="text-gray-500">更新:</span>
                                    <div className="text-gray-700">{formatTimestamp(lag.updatedAt)}</div>
                                  </div>
                                )}
                              </div>
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
