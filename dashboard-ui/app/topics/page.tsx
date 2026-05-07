'use client';

import React, { useState, useEffect, useCallback, Suspense } from 'react';
import {useRouter, useSearchParams} from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { TopicMetrics, Message, PartitionStats, ConsumerGroupMetrics, GroupMember } from '@/lib/types';
import { formatNumber, formatBytes, formatTimestamp } from '@/lib/utils';
import { StatCard } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
  ArrowLeft,
  RefreshCw,
  Database,
  MessageSquare,
  HardDrive,
  Hash,
  Search,
  ChevronDown,
  ChevronUp,
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Maximize2,
  Copy,
  Download,
  X,
  Send,
  Trash2,
  Users,
} from 'lucide-react';
import Link from 'next/link';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { JsonViewer } from '@/components/ui/json-viewer';
import { Checkbox } from '@/components/ui/checkbox';
import { Badge } from '@/components/ui/badge';
import { cn } from '@/lib/utils';

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function TopicDetailContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const topicName = safeDecodeURIComponent(searchParams.get('name') || '');

  const [topic, setTopic] = useState<TopicMetrics | null>(null);
  const [hasSubscribers, setHasSubscribers] = useState(false);
  const [subscribedGroups, setSubscribedGroups] = useState<ConsumerGroupMetrics[]>([]);
  const [deletingTopic, setDeletingTopic] = useState(false);
  const [messages, setMessages] = useState<Message[]>([]);
  const [loading, setLoading] = useState(true);
  const [messagesLoading, setMessagesLoading] = useState(false);
  const [showLoading, setShowLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [partitionStats, setPartitionStats] = useState<PartitionStats[]>([]);

  // 消息查询参数
  const [selectedPartition, setSelectedPartition] = useState<string>('');
  const [searchText, setSearchText] = useState('');
  const [messageLimit, setMessageLimit] = useState(10);

  // 添加实际搜索条件的状态
  const [activeSearchParams, setActiveSearchParams] = useState({
    partition: '',
    search: '',
    limit: 10
  });

  const [expandedMessages, setExpandedMessages] = useState<Set<string>>(new Set());
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [resending, setResending] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [totalMessages, setTotalMessages] = useState(0);

  const memberConsumesTopic = useCallback((member: GroupMember, targetTopic: string): boolean => {
    if (member.subscribedTopics?.includes(targetTopic)) {
      return true;
    }

    const assignment = member.assignment;
    if (!assignment || typeof assignment !== 'object') {
      return false;
    }

    if (Array.isArray(assignment)) {
      return assignment.some(item => item.Topic === targetTopic);
    }

    return Object.prototype.hasOwnProperty.call(assignment, targetTopic);
  }, []);

  // 监听加载状态变化
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout>;

    if (messagesLoading) {
      timer = setTimeout(() => {
        setShowLoading(true);
      }, 2000);
    } else {
      setShowLoading(false);
    }

    return () => {
      clearTimeout(timer);
    };
  }, [messagesLoading]);

  // 加载Topic详情
  const loadTopicDetail = useCallback(async () => {
    try {
      setLoading(true);
      const topicData = await DBMQAPIClient.getTopic(topicName);
      setTopic(topicData);
      const topicGroups = await DBMQAPIClient.getTopicConsumerGroups(topicName);
      setHasSubscribers(topicGroups.length > 0);
      setSubscribedGroups(topicGroups);
      setError(null);

      if (topicData.partitionStats && topicData.partitionStats.length > 0) {
        setPartitionStats(topicData.partitionStats);
      } else if (topicData.partitionCount) {
        const statsPromises: Promise<PartitionStats>[] = [];
        for (let i = 0; i < topicData.partitionCount; i++) {
          statsPromises.push(DBMQAPIClient.getPartitionStats(topicName, i));
        }

        try {
          const stats = await Promise.all(statsPromises);
          setPartitionStats(stats);
        } catch (err) {
          console.warn('Failed to load partition stats:', err);
          // 分区统计信息加载失败不影响主要功能
        }
      }
    } catch (err) {
      console.error('Failed to load topic detail:', err);
      setError(err instanceof Error ? err.message : '加载Topic详情失败');
    } finally {
      setLoading(false);
    }
  }, [topicName]);

  const handleDeleteTopic = useCallback(async () => {
    if (hasSubscribers || deletingTopic) return;
    if (!confirm(`确定要删除 Topic ${topicName} 吗？`)) return;

    setDeletingTopic(true);
    try {
      await DBMQAPIClient.deleteTopic(topicName);
      router.push('/');
    } catch (err) {
      console.error('Failed to delete topic:', err);
      alert('删除 Topic 失败: ' + (err instanceof Error ? err.message : '未知错误'));
    } finally {
      setDeletingTopic(false);
    }
  }, [topicName, hasSubscribers, deletingTopic, router]);

  // 加载消息列表
  const loadMessages = useCallback(async () => {
    if (messagesLoading) return; // 防止重复加载

    try {
      setMessagesLoading(true);
      const params: Record<string, string | number> = {
        limit: activeSearchParams.limit,
        offset: (currentPage - 1) * activeSearchParams.limit
      };

      if (activeSearchParams.partition) {
        params.partition = parseInt(activeSearchParams.partition);
      }
      if (activeSearchParams.search) {
        params.search = activeSearchParams.search;
      }

      const result = await DBMQAPIClient.getTopicMessages(topicName, params);
      setMessages(result.messages);
      setTotalMessages(result.total || 0);
      setTotalPages(Math.ceil((result.total || 0) / activeSearchParams.limit));

      // 检查全局状态
      const allExpanded = document.querySelector('[data-expand-all="true"]') !== null;

      // 根据全局状态设置新消息的状态
      if (allExpanded) {
        setExpandedMessages(new Set(result.messages.map(m => m.id)));
      }
    } catch (err) {
      console.error('Failed to load messages:', err);
      setError(err instanceof Error ? err.message : '加载消息失败');
    } finally {
      setMessagesLoading(false);
    }
  }, [topicName, currentPage, activeSearchParams]);

  // 处理搜索按钮点击
  const handleSearch = useCallback(() => {
    // 如果搜索条件没有变化，不需要重新加载
    if (
        activeSearchParams.search === searchText &&
        activeSearchParams.limit === messageLimit &&
        activeSearchParams.partition === selectedPartition
    ) {
      return;
    }

    // 更新搜索参数并重置页码
    setCurrentPage(1);
    setActiveSearchParams({
      search: searchText,
      limit: messageLimit,
      partition: selectedPartition
    });
  }, [searchText, messageLimit, selectedPartition, activeSearchParams]);

  // 切换消息展开状态
  const toggleMessage = useCallback((messageId: string) => {
    const newExpanded = new Set(expandedMessages);
    if (newExpanded.has(messageId)) {
      newExpanded.delete(messageId);
    } else {
      newExpanded.add(messageId);
    }
    setExpandedMessages(newExpanded);
  }, [expandedMessages]);

  // 全选/取消全选
  const handleSelectAll = useCallback(() => {
    if (selectedIds.size === messages.length && messages.length > 0) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(messages.map(m => m.id)));
    }
  }, [messages, selectedIds]);

  // 切换单个选择
  const toggleSelection = useCallback((id: string) => {
    setSelectedIds(prev => {
      const newSet = new Set(prev);
      if (newSet.has(id)) {
        newSet.delete(id);
      } else {
        newSet.add(id);
      }
      return newSet;
    });
  }, []);

  // 复制选中的消息
  const handleCopySelected = useCallback(() => {
    const selectedMessages = messages.filter(m => selectedIds.has(m.id));
    const text = selectedMessages.map(m => m.value).join('\n\n');
    navigator.clipboard.writeText(text);
  }, [messages, selectedIds]);

  // 导出选中的消息
  const handleExportSelected = useCallback(() => {
    const selectedMessages = messages.filter(m => selectedIds.has(m.id));
    const blob = new Blob([JSON.stringify(selectedMessages, null, 2)], {
      type: 'application/json'
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `messages-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
  }, [messages, selectedIds]);

  // 重发选中的消息
  const handleResendSelected = useCallback(async () => {
    if (selectedIds.size === 0) return;

    setResending(true);
    try {
      const selectedMessages = messages.filter(m => selectedIds.has(m.id));
      const request = {
        messages: selectedMessages.map(m => ({
          topic: m.topic,
          messageId: parseInt(m.id),
          key: undefined, // 使用原消息的 key
        })),
      };

      const result = await DBMQAPIClient.resendMessages(request);

      // 显示结果
      alert(`重发完成！\n成功: ${result.successCount} 条\n失败: ${result.failedCount} 条`);

      // 清除选择
      setSelectedIds(new Set());

      // 刷新消息列表
      await loadMessages();
    } catch (err) {
      console.error('Failed to resend messages:', err);
      alert(`重发失败: ${err instanceof Error ? err.message : '未知错误'}`);
    } finally {
      setResending(false);
    }
  }, [messages, selectedIds]);

  // 展开/折叠所有消息
  const toggleAllMessages = useCallback(() => {
    if (expandedMessages.size === messages.length) {
      setExpandedMessages(new Set());
      document.querySelector('[data-expand-all]')?.setAttribute('data-expand-all', 'false');
    } else {
      setExpandedMessages(new Set(messages.map(m => m.id)));
      document.querySelector('[data-expand-all]')?.setAttribute('data-expand-all', 'true');
    }
  }, [messages, expandedMessages]);

  // 页码变更处理
  const handlePageChange = useCallback((page: number) => {
    if (page === currentPage || messagesLoading) return;
    setCurrentPage(page);
  }, [currentPage, messagesLoading]);

  // 计算要显示的页码范围
  const getPageNumbers = useCallback(() => {
    const pageNumbers: number[] = [];
    const maxVisiblePages = 5;

    if (totalPages <= maxVisiblePages) {
      // 如果总页数小于等于最大显示页数，显示所有页码
      for (let i = 1; i <= totalPages; i++) {
        pageNumbers.push(i);
      }
    } else {
      // 总是显示第一页
      pageNumbers.push(1);

      let start = Math.max(2, currentPage - 1);
      let end = Math.min(totalPages - 1, start + 2);

      // 调整start以确保显示3个数字
      if (end === totalPages - 1) {
        start = Math.max(2, end - 2);
      }

      // 添加省略号
      if (start > 2) {
        pageNumbers.push(-1); // 用负数表示省略号
      }

      // 添加中间的页码
      for (let i = start; i <= end; i++) {
        pageNumbers.push(i);
      }

      // 添加省略号
      if (end < totalPages - 1) {
        pageNumbers.push(-2); // 用负数表示省略号
      }

      // 总是显示最后一页
      pageNumbers.push(totalPages);
    }

    return pageNumbers;
  }, [currentPage, totalPages]);

  // 当页码或活动搜索参数变化时重新加载数据
  useEffect(() => {
    const shouldLoad = currentPage > 0 && activeSearchParams.limit > 0;

    if (shouldLoad) {
      loadMessages();
    }
  }, [currentPage, activeSearchParams, loadMessages]);

  // 初始加载
  useEffect(() => {
    if (topicName) {
      loadTopicDetail();
      loadMessages();
    } else {
      // 如果没有 topicName，说明是直接访问 /topics，设置 loading 为 false
      setLoading(false);
    }
  }, [topicName, loadTopicDetail, loadMessages]);

  // 处理分区选择变化
  const handlePartitionChange = useCallback((partition: string) => {
    setSelectedPartition(partition);
  }, []);

  if (loading && !topic) {
    return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
            <p className="text-gray-600">加载Topic详情中...</p>
          </div>
        </div>
    );
  }

  if (error && !topic) {
    return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center">
            <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
              <p className="font-bold">错误</p>
              <p>{error}</p>
            </div>
            <Button onClick={() => {
              loadTopicDetail();
              loadMessages();
            }} variant="primary">
              重试
            </Button>
          </div>
        </div>
    );
  }

  // 如果没有 topicName，显示提示
  if (!topicName) {
    return (
        <div className="min-h-screen bg-gray-50 flex items-center justify-center">
          <div className="text-center max-w-md mx-auto p-8">
            <Database className="h-16 w-16 mx-auto mb-4 text-gray-400" />
            <h2 className="text-xl font-medium text-gray-900 mb-2">请选择一个 Topic</h2>
            <p className="text-gray-600 mb-6">
              请从首页的 Topic 列表中选择一个 Topic 查看详情
            </p>
            <Link href="/">
              <Button variant="primary">
                <ArrowLeft className="h-4 w-4 mr-2" />
                返回首页
              </Button>
            </Link>
          </div>
        </div>
    );
  }

  return (
      <div className="min-h-screen bg-gray-50">
        {/* 头部 */}
        <header className="bg-white shadow-sm">
          <div className="page-shell">
            <div className="flex items-center py-4">
              <Link
                  href="/"
                  className="inline-flex items-center text-gray-600 hover:text-gray-800 mr-4 transition-colors"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                返回
              </Link>
              <div className="flex-1">
                <h1 className="text-xl font-medium text-gray-900">Topic 详情</h1>
                <p className="text-sm text-gray-600">{topicName}</p>
              </div>
              <div className="flex items-center gap-2">
                <Button
                    size="sm"
                    variant="outline"
                    className={hasSubscribers ? 'text-gray-300' : 'text-red-600 hover:text-red-800 border-red-200 hover:bg-red-50'}
                    disabled={hasSubscribers || deletingTopic}
                    title={hasSubscribers ? 'Topic 有消费组订阅，无法删除' : '删除 Topic'}
                    onClick={handleDeleteTopic}
                >
                  <Trash2 className="h-4 w-4 mr-1" />
                  {deletingTopic ? '删除中...' : '删除'}
                </Button>
                <Button
                  onClick={() => {
                    loadTopicDetail();
                    loadMessages();
                  }}
                  size="sm"
                  variant="outline"
                  className="text-gray-600 hover:text-gray-800"
                >
                  <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
                </Button>
              </div>
            </div>
          </div>
        </header>

        <main className="page-shell py-6">
          {/* 统计卡片 */}
          <div className="grid grid-cols-2 md:grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
            <StatCard
                title="分区数"
                value={topic?.partitionCount || topic?.partitions?.length || 0}
                icon={<Database className="h-5 w-5 text-blue-500" />}
                className="bg-blue-50 border-none"
            />
            <StatCard
                title="消息总数"
                value={formatNumber(topic?.messageCount || 0)}
                icon={<MessageSquare className="h-5 w-5 text-purple-500" />}
                className="bg-purple-50 border-none"
            />
            <StatCard
                title="存储大小"
                value={formatBytes((topic as TopicMetrics & { sizeBytes?: number })?.sizeBytes || 0)}
                icon={<HardDrive className="h-5 w-5 text-orange-500" />}
                className="bg-orange-50 border-none"
            />
            <StatCard
                title="最新ID"
                value={formatNumber((topic as TopicMetrics & { latestOffset?: number })?.latestOffset || 0)}
                icon={<Hash className="h-5 w-5 text-indigo-500" />}
                className="bg-indigo-50 border-none"
            />
          </div>

          {/* 消费组 */}
          <div className="bg-white rounded-lg mb-6">
            <div className="px-4 py-3 border-b border-gray-100">
              <h3 className="text-sm font-medium text-gray-900 flex items-center">
                <Users className="h-4 w-4 mr-2 text-blue-500" />
                消费组
              </h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                <tr className="bg-gray-50">
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">消费组</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">分区数</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">待消费</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">消费者</th>
                </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                {subscribedGroups.length > 0 ? (
                  subscribedGroups.map((group) => {
                    const groupId = group.groupId || group.name || '--';
                    const topicLags = (group.partitionLags || []).filter(lag => lag.topic === topicName);
                    const totalLag = topicLags.reduce((sum, lag) => sum + (lag.lag || 0), 0);
                    const topicMembers = (group.members || []).filter(member => memberConsumesTopic(member, topicName));

                    return (
                      <tr key={groupId} className="hover:bg-gray-50">
                        <td className="px-4 py-2">
                          <Link
                            href={`/consumer-groups?id=${encodeURIComponent(groupId)}`}
                            className="text-sm font-medium text-blue-600 hover:text-blue-800"
                          >
                            {groupId}
                          </Link>
                        </td>
                        <td className="px-4 py-2 text-sm text-gray-700">
                          {topicLags.length}
                        </td>
                        <td className="px-4 py-2 text-sm text-gray-700">
                          {formatNumber(totalLag)}
                        </td>
                        <td className="px-4 py-2">
                          {topicMembers.length > 0 ? (
                            <div className="flex flex-wrap gap-1">
                              {topicMembers.map((member) => (
                                <Badge key={member.memberId} variant="outline" className="max-w-[260px] truncate bg-blue-50 text-blue-700 border-blue-200">
                                  {member.memberId}
                                </Badge>
                              ))}
                            </div>
                          ) : (
                            <span className="text-xs text-gray-500">暂无成员数据</span>
                          )}
                        </td>
                      </tr>
                    );
                  })
                ) : (
                  <tr>
                    <td colSpan={4} className="px-4 py-4 text-sm text-gray-500 text-center">
                      暂无消费组消费此 Topic
                    </td>
                  </tr>
                )}
                </tbody>
              </table>
            </div>
          </div>

          {/* 分区详情 */}
          <div className="bg-white rounded-lg mb-6">
            <div className="px-4 py-3 border-b border-gray-100">
              <h3 className="text-sm font-medium text-gray-900 flex items-center">
                <Database className="h-4 w-4 mr-2 text-blue-500" />
                分区详情
              </h3>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                <tr className="bg-gray-50">
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">分区ID</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">消息ID范围</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">消息总数</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">存储大小</th>
                  <th className="px-4 py-2 text-left text-xs font-medium text-gray-500">时间范围</th>
                </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                {partitionStats.length > 0 ? (
                    partitionStats.map((stats) => (
                        <tr key={stats.partition} className="hover:bg-gray-50">
                          <td className="px-4 py-2">
                            <div className="flex items-center">
                              <Database className="h-4 w-4 mr-2 text-blue-500" />
                              <span className="text-sm font-medium text-gray-900">分区 {stats.partition}</span>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="text-sm space-y-1">
                              <div>
                                <span className="text-gray-500">首个:</span>
                                <span className="ml-1 font-mono text-gray-700">
                              {stats.firstMessageId === -1 ? '无' : formatNumber(stats.firstMessageId)}
                            </span>
                              </div>
                              <div>
                                <span className="text-gray-500">最新:</span>
                                <span className="ml-1 font-mono text-gray-700">
                              {stats.lastMessageId === -1 ? '无' : formatNumber(stats.lastMessageId)}
                            </span>
                              </div>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="flex items-center">
                              <Hash className="h-4 w-4 mr-1 text-purple-500" />
                              <span className="text-sm font-medium text-gray-900">
                            {formatNumber(stats.messageCount)}
                          </span>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="flex items-center">
                              <HardDrive className="h-4 w-4 mr-1 text-orange-500" />
                              <span className="text-sm font-medium text-gray-900">
                            {formatBytes(stats.sizeBytes)}
                          </span>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="text-xs space-y-1 text-gray-500">
                              {stats.createdAt && (
                                  <div>
                                    <span>首条:</span>
                                    <span className="ml-1">{formatTimestamp(stats.createdAt)}</span>
                                  </div>
                              )}
                              {stats.updatedAt && (
                                  <div>
                                    <span>最新:</span>
                                    <span className="ml-1">{formatTimestamp(stats.updatedAt)}</span>
                                  </div>
                              )}
                            </div>
                          </td>
                        </tr>
                    ))
                ) : topic?.partitionCount ? (
                    // 如果分区统计信息还在加载中，显示骨架屏
                    Array.from({ length: topic.partitionCount }, (_, index) => (
                        <tr key={index} className="animate-pulse">
                          <td className="px-4 py-2">
                            <div className="flex items-center">
                              <div className="h-4 w-4 bg-gray-200 rounded mr-2"></div>
                              <div className="h-4 w-16 bg-gray-200 rounded"></div>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="space-y-1">
                              <div className="h-3 w-20 bg-gray-200 rounded"></div>
                              <div className="h-3 w-20 bg-gray-200 rounded"></div>
                            </div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="h-4 w-12 bg-gray-200 rounded"></div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="h-4 w-16 bg-gray-200 rounded"></div>
                          </td>
                          <td className="px-4 py-2">
                            <div className="space-y-1">
                              <div className="h-3 w-24 bg-gray-200 rounded"></div>
                              <div className="h-3 w-24 bg-gray-200 rounded"></div>
                            </div>
                          </td>
                        </tr>
                    ))
                ) : (
                    <tr>
                      <td colSpan={5} className="px-4 py-4 text-sm text-gray-500 text-center">
                        暂无分区数据
                      </td>
                    </tr>
                )}
                </tbody>
              </table>
            </div>
          </div>

          {/* 消息浏览器 */}
          <div className="bg-white rounded-lg">
            <div className="px-4 py-3 border-b border-gray-100">
              <div className="flex justify-between items-center">
                <h3 className="text-sm font-medium text-gray-900 flex items-center">
                  <MessageSquare className="h-4 w-4 mr-2 text-blue-500" />
                  消息浏览器
                </h3>
                <div className="flex items-center space-x-2">
                  <Button
                      variant="outline"
                      size="sm"
                      onClick={toggleAllMessages}
                      className="text-gray-600 hover:text-gray-800"
                      data-expand-all={expandedMessages.size === messages.length}
                  >
                    <Maximize2 className="h-4 w-4 mr-1" />
                    {expandedMessages.size === messages.length ? '折叠全部' : '展开全部'}
                  </Button>
                </div>
              </div>
            </div>

            <div className="p-4">
              {/* 搜索控件 */}
              <div className="flex flex-wrap gap-4 mb-6">
                <div className="flex-1 min-w-48">
                  <label className="block text-xs font-medium text-gray-500 mb-1">
                    分区
                  </label>
                  <select
                      value={selectedPartition}
                      onChange={(e) => handlePartitionChange(e.target.value)}
                      className="w-full px-3 py-1.5 border border-gray-200 rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="">所有分区</option>
                    {topic?.partitions?.map((partition, index) => (
                        <option key={index} value={partition.partition || index}>
                          分区 {partition.partition || index}
                        </option>
                    ))}
                  </select>
                </div>

                <div className="flex-1 min-w-48">
                  <label className="block text-xs font-medium text-gray-500 mb-1">
                    搜索
                  </label>
                  <input
                      type="text"
                      value={searchText}
                      onChange={(e) => setSearchText(e.target.value)}
                      placeholder="搜索消息键或内容..."
                      className="w-full px-3 py-1.5 border border-gray-200 rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                  />
                </div>

                <div className="w-24">
                  <label className="block text-xs font-medium text-gray-500 mb-1">
                    数量
                  </label>
                  <select
                      value={messageLimit}
                      onChange={(e) => {
                        const newLimit = parseInt(e.target.value);
                        setMessageLimit(newLimit);
                      }}
                      className="w-full px-3 py-1.5 border border-gray-200 rounded-md text-sm focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value={10}>10</option>
                    <option value={20}>20</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                  </select>
                </div>

                <div className="flex items-end">
                  <Button
                      onClick={handleSearch}
                      disabled={showLoading}
                      variant="primary"
                      size="sm"
                      className="bg-blue-600 hover:bg-blue-700 text-white"
                  >
                    <Search className="h-4 w-4 mr-1" />
                    查询
                  </Button>
                </div>
              </div>

              {/* 批量操作工具栏 */}
              {selectedIds.size > 0 && (
                <div className="sticky top-0 z-10 bg-blue-50 border border-blue-200 rounded-md p-3 mb-4 flex items-center justify-between">
                  <div className="flex items-center space-x-4">
                    <span className="text-sm font-medium text-blue-900">
                      已选中 {selectedIds.size} 条消息
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleResendSelected}
                      disabled={resending}
                    >
                      {resending ? (
                        <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                      ) : (
                        <Send className="h-4 w-4 mr-2" />
                      )}
                      {resending ? '重发中...' : '重发'}
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleCopySelected}
                    >
                      <Copy className="h-4 w-4 mr-2" />
                      复制
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={handleExportSelected}
                    >
                      <Download className="h-4 w-4 mr-2" />
                      导出 JSON
                    </Button>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setSelectedIds(new Set())}
                  >
                    <X className="h-4 w-4 mr-2" />
                    清除
                  </Button>
                </div>
              )}

              {/* 消息列表 */}
              <div className="mb-4 overflow-x-auto">
                {messagesLoading && showLoading ? (
                    <div className="min-h-[400px] flex items-center justify-center">
                      <div className="text-center">
                        <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
                        <p className="text-sm text-gray-500">加载消息中...</p>
                      </div>
                    </div>
                ) : messages.length === 0 && !messagesLoading ? (
                    <div className="min-h-[400px] flex items-center justify-center">
                      <div className="text-center">
                        <MessageSquare className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                        <p className="text-sm text-gray-500">暂无消息数据</p>
                      </div>
                    </div>
                ) : (
                    <Table className="table-fixed">
                      <TableHeader>
                        <TableRow>
                          <TableHead className="w-12">
                            <Checkbox
                              checked={selectedIds.size === messages.length && messages.length > 0}
                              onChange={handleSelectAll}
                            />
                          </TableHead>
                          <TableHead className="w-24">Offset</TableHead>
                          <TableHead className="w-20">分区</TableHead>
                          <TableHead className="w-48">时间戳</TableHead>
                          <TableHead>Key</TableHead>
                          <TableHead className="w-24">大小</TableHead>
                          <TableHead className="w-12"></TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {messages.map((message) => (
                          <React.Fragment key={message.id}>
                            <TableRow
                              className={cn(
                                "cursor-pointer",
                                expandedMessages.has(message.id) && "bg-blue-50"
                              )}
                              onClick={() => toggleMessage(message.id)}
                            >
                              <TableCell onClick={e => e.stopPropagation()}>
                                <Checkbox
                                  checked={selectedIds.has(message.id)}
                                  onChange={() => toggleSelection(message.id)}
                                />
                              </TableCell>
                              <TableCell className="font-mono text-xs">
                                {message.offset}
                              </TableCell>
                              <TableCell>
                                <Badge variant="outline">P{message.partition}</Badge>
                              </TableCell>
                              <TableCell className="text-xs text-gray-600">
                                {formatTimestamp(message.timestamp)}
                              </TableCell>
                              <TableCell className="truncate max-w-xs">
                                {message.key || <span className="text-gray-400">(无键)</span>}
                              </TableCell>
                              <TableCell className="text-xs">
                                {formatBytes(message.size)}
                              </TableCell>
                              <TableCell>
                                {expandedMessages.has(message.id) ? (
                                  <ChevronUp className="h-4 w-4 text-gray-400" />
                                ) : (
                                  <ChevronDown className="h-4 w-4 text-gray-400" />
                                )}
                              </TableCell>
                            </TableRow>

                            {/* 展开行 */}
                            {expandedMessages.has(message.id) && (
                              <TableRow key={`${message.id}-expanded`}>
                                <TableCell colSpan={7} className="bg-gray-50 p-0 max-w-0">
                                  <div className="min-w-0 max-w-full p-4 space-y-3">
                                    {/* 消息元数据 */}
                                    <div className="flex flex-wrap gap-x-6 gap-y-1 text-xs text-gray-500">
                                      <span>Topic: <span className="font-mono text-gray-700">{message.topic}</span></span>
                                      <span>Partition: <span className="font-mono text-gray-700">{message.partition}</span></span>
                                      <span>Offset: <span className="font-mono text-gray-700">{message.offset}</span></span>
                                      <span>Size: <span className="font-mono text-gray-700">{formatBytes(message.size)}</span></span>
                                      <span>Time: <span className="font-mono text-gray-700">{formatTimestamp(message.timestamp)}</span></span>
                                    </div>

                                    {/* Headers */}
                                    {message.headers && Object.keys(message.headers).length > 0 && (
                                      <div>
                                        <div className="text-xs font-medium text-gray-600 mb-1">Headers</div>
                                        <div className="bg-white border border-gray-200 rounded-md p-2">
                                          <div className="flex flex-wrap gap-2">
                                            {Object.entries(message.headers).map(([k, v]) => (
                                              <span key={k} className="inline-flex items-center text-xs font-mono bg-gray-100 rounded px-2 py-0.5">
                                                <span className="text-purple-600">{k}</span>
                                                <span className="text-gray-400 mx-1">=</span>
                                                <span className="text-gray-700">{v}</span>
                                              </span>
                                            ))}
                                          </div>
                                        </div>
                                      </div>
                                    )}

                                    {/* Value (消息体) */}
                                    <div className="min-w-0 max-w-full">
                                      <div className="text-xs font-medium text-gray-600 mb-1">Value</div>
                                      <JsonViewer
                                        data={message.value}
                                        collapsed={false}
                                        theme="light"
                                        className="max-w-full"
                                      />
                                    </div>
                                  </div>
                                </TableCell>
                              </TableRow>
                            )}
                          </React.Fragment>
                        ))}
                      </TableBody>
                    </Table>
                )}
              </div>

              {/* 分页控件 */}
              {messages.length > 0 && (
                  <div className="flex items-center justify-between border-t border-gray-100 pt-4">
                    <div className="text-sm text-gray-500">
                      共 {totalMessages} 条消息，{totalPages} 页
                    </div>
                    <div className="flex items-center space-x-2">
                      <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(1)}
                          disabled={currentPage === 1}
                          className="cursor-pointer"
                      >
                        <ChevronsLeft className="h-4 w-4" />
                      </Button>
                      <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(currentPage - 1)}
                          disabled={currentPage === 1}
                          className="cursor-pointer"
                      >
                        <ChevronLeft className="h-4 w-4" />
                      </Button>

                      <div className="flex items-center space-x-1">
                        {getPageNumbers().map((pageNum, index) => {
                          if (pageNum < 0) {
                            return (
                                <span key={pageNum} className="px-2 text-gray-400">
                            ...
                          </span>
                            );
                          }

                          return (
                              <Button
                                  key={index}
                                  variant={currentPage === pageNum ? "default" : "outline"}
                                  size="sm"
                                  onClick={() => handlePageChange(pageNum)}
                                  className={
                                    currentPage === pageNum
                                        ? "bg-blue-600 text-white hover:bg-blue-700 hover:text-white cursor-pointer min-w-[32px]"
                                        : "hover:bg-gray-100 cursor-pointer min-w-[32px]"
                                  }
                              >
                                {pageNum}
                              </Button>
                          );
                        })}
                      </div>

                      <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(currentPage + 1)}
                          disabled={currentPage === totalPages}
                          className="cursor-pointer"
                      >
                        <ChevronRight className="h-4 w-4" />
                      </Button>
                      <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handlePageChange(totalPages)}
                          disabled={currentPage === totalPages}
                          className="cursor-pointer"
                      >
                        <ChevronsRight className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
              )}
            </div>
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

export default function TopicDetailPage() {
  return (
      <Suspense fallback={<LoadingFallback />}>
        <TopicDetailContent />
      </Suspense>
  );
}
