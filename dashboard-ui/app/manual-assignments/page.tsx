'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { ManualAssignment, CreateManualAssignmentRequest, ConsumerGroupMetrics, TopicMetrics } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ArrowLeft, RefreshCw, Plus, Trash2, Info, Filter } from 'lucide-react';
import Link from 'next/link';
import { apiConfig } from '@/config/api.config';

export default function ManualAssignmentsPage() {
  const router = useRouter();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroupMetrics[]>([]);
  const [topics, setTopics] = useState<TopicMetrics[]>([]);
  const [filterGroupId, setFilterGroupId] = useState<string>(''); // 改为过滤器，不是必选
  const [allAssignments, setAllAssignments] = useState<ManualAssignment[]>([]);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [formLoading, setFormLoading] = useState(false);
  const [rebalancing, setRebalancing] = useState<{[key: string]: boolean}>({});
  const [formData, setFormData] = useState<CreateManualAssignmentRequest>({
    group_id: '',
    consumer_id_pattern: '',
    topic: '',
    partition: 0,
  });

  // 加载所有数据
  useEffect(() => {
    const loadInitialData = async () => {
      try {
        console.log('🔍 开始加载数据...');
        const [groupsData, topicsData] = await Promise.all([
          DBMQAPIClient.getConsumerGroups(),
          DBMQAPIClient.getTopics(true),
        ]);
        console.log('✅ 消费组数据:', groupsData);
        console.log('✅ Topic 数据:', topicsData);
        setConsumerGroups(groupsData);
        setTopics(topicsData);

        // 加载所有消费组的分配规则
        await loadAllAssignments(groupsData);

        setError(null);
      } catch (error) {
        console.error('❌ 加载数据失败:', error);
        const errorMsg = error instanceof Error ? error.message : '加载数据失败';
        setError(errorMsg);
      } finally {
        setLoading(false);
      }
    };
    loadInitialData();
  }, []);

  // 加载所有消费组的分配规则
  const loadAllAssignments = async (groups?: ConsumerGroupMetrics[]) => {
    const groupsToLoad = groups || consumerGroups;
    if (groupsToLoad.length === 0) {
      setAllAssignments([]);
      return;
    }

    try {
      // 并行加载所有消费组的规则
      const allPromises = groupsToLoad.map(async (group) => {
        const groupId = group.groupId || group.name || '';
        try {
          const assignments = await DBMQAPIClient.getManualAssignments(groupId);
          return assignments;
        } catch (error) {
          console.error(`Failed to load assignments for ${groupId}:`, error);
          return [];
        }
      });

      const results = await Promise.all(allPromises);
      const combined = results.flat();
      console.log('✅ 加载了所有分配规则:', combined.length, '条');
      setAllAssignments(combined);
    } catch (error) {
      console.error('Failed to load all assignments:', error);
      setAllAssignments([]);
    }
  };

  const handleCreateSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.group_id || !formData.consumer_id_pattern.trim() || !formData.topic) {
      alert('请填写所有必填字段');
      return;
    }

    setFormLoading(true);
    try {
      await DBMQAPIClient.createManualAssignment(formData);
      setShowCreateForm(false);
      setFormData({
        group_id: '',
        consumer_id_pattern: '',
        topic: '',
        partition: 0,
      });
      await loadAllAssignments();
    } catch (error) {
      console.error('Failed to create assignment:', error);
      alert('创建分配规则失败: ' + (error instanceof Error ? error.message : '未知错误'));
    } finally {
      setFormLoading(false);
    }
  };

  const handleDelete = async (id: number) => {
    if (!confirm('确定要删除这条分配规则吗？')) return;

    try {
      await DBMQAPIClient.deleteManualAssignment(id);
      await loadAllAssignments();
    } catch (error) {
      console.error('Failed to delete assignment:', error);
      alert('删除分配规则失败: ' + (error instanceof Error ? error.message : '未知错误'));
    }
  };

  const handleTriggerRebalance = async (groupId: string) => {
    setRebalancing(prev => ({ ...prev, [groupId]: true }));
    try {
      await DBMQAPIClient.triggerRebalance(groupId);
      alert(`消费组 ${groupId} 的重新均衡已触发，更改将在 10 秒内生效`);
      // 3 秒后自动刷新列表
      setTimeout(() => {
        loadAllAssignments();
      }, 3000);
    } catch (error) {
      console.error('Failed to trigger rebalance:', error);
      alert('触发重新均衡失败: ' + (error instanceof Error ? error.message : '未知错误'));
    } finally {
      setRebalancing(prev => ({ ...prev, [groupId]: false }));
    }
  };

  // 获取选中Topic的分区数
  const getPartitionCount = (): number => {
    const topic = topics.find(t => (t.name || t.topicName) === formData.topic);
    return topic?.partitionCount || topic?.partitions?.length || 1;
  };

  // 按消费组分组分配规则
  const groupedAssignments = allAssignments.reduce((acc, assignment) => {
    const groupId = assignment.group_id;
    if (!acc[groupId]) {
      acc[groupId] = [];
    }
    acc[groupId].push(assignment);
    return acc;
  }, {} as {[key: string]: ManualAssignment[]});

  // 应用过滤
  const filteredGroupIds = filterGroupId
    ? Object.keys(groupedAssignments).filter(gid => gid === filterGroupId)
    : Object.keys(groupedAssignments).sort();

  // 获取所有消费组（包括没有规则的）
  const allGroupIds = consumerGroups.map(g => g.groupId || g.name || '').filter(Boolean);
  const displayGroupIds = filterGroupId
    ? (allGroupIds.includes(filterGroupId) ? [filterGroupId] : [])
    : allGroupIds;

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载数据中...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="max-w-[98%] mx-auto px-4">
        {/* 头部 */}
        <div className="mb-8 flex justify-between items-start">
          <div>
            <Link
              href="/"
              className="inline-flex items-center text-gray-600 hover:text-gray-800 mb-4 transition-colors"
            >
              <ArrowLeft className="h-4 w-4 mr-2" />
              返回仪表板
            </Link>
            <h1 className="text-2xl font-semibold text-gray-900">手动分区分配管理</h1>
            <p className="text-gray-600 mt-1">
              查看和管理所有消费组的手动分区分配规则
              {allAssignments.length > 0 && (
                <span className="ml-2 text-blue-600 font-medium">
                  （共 {allAssignments.length} 条规则）
                </span>
              )}
            </p>
          </div>
          <div className="flex gap-2">
            <Button
              size="sm"
              variant="outline"
              className="text-green-600 hover:text-green-800"
              onClick={() => setShowCreateForm(!showCreateForm)}
            >
              <Plus className="h-4 w-4 mr-1" />
              {showCreateForm ? '取消添加' : '添加规则'}
            </Button>
            <Button
              onClick={() => loadAllAssignments()}
              size="sm"
              variant="outline"
            >
              <RefreshCw className="h-4 w-4 mr-1" />
              刷新全部
            </Button>
          </div>
        </div>

        {/* 错误提示 */}
        {error && (
          <div className="mb-6 bg-red-50 border border-red-200 rounded-md p-4">
            <div className="flex items-start">
              <div className="flex-shrink-0">
                <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                  <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
                </svg>
              </div>
              <div className="ml-3">
                <h3 className="text-sm font-medium text-red-800">加载失败</h3>
                <p className="mt-1 text-sm text-red-700">{error}</p>
                <p className="mt-2 text-xs text-red-600">
                  请检查：
                  <br />• 后端服务是否运行（http://localhost:8081）
                  <br />• 浏览器控制台是否有 CORS 错误
                  <br />• API 配置是否正确
                </p>
              </div>
            </div>
          </div>
        )}

        {/* 创建表单 */}
        {showCreateForm && (
          <Card className="shadow-sm mb-6">
            <CardHeader className="border-b border-gray-100">
              <CardTitle className="text-lg">添加新规则</CardTitle>
            </CardHeader>
            <CardContent className="pt-6">
              <form onSubmit={handleCreateSubmit} className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  <div>
                    <label htmlFor="group_id" className="block text-xs font-medium text-gray-600 mb-1">
                      消费组 *
                    </label>
                    <select
                      id="group_id"
                      required
                      value={formData.group_id}
                      onChange={(e) => setFormData(prev => ({ ...prev, group_id: e.target.value }))}
                      className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                    >
                      <option value="">-- 选择消费组 --</option>
                      {consumerGroups.map((group) => {
                        const groupId = group.groupId || group.name || '';
                        return (
                          <option key={groupId} value={groupId}>
                            {groupId}
                          </option>
                        );
                      })}
                    </select>
                  </div>
                  <div>
                    <label htmlFor="consumer_id_pattern" className="block text-xs font-medium text-gray-600 mb-1">
                      消费者ID模式 *
                    </label>
                    <input
                      type="text"
                      id="consumer_id_pattern"
                      required
                      value={formData.consumer_id_pattern}
                      onChange={(e) => setFormData(prev => ({ ...prev, consumer_id_pattern: e.target.value }))}
                      className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                      placeholder="例如: consumer-1, consumer-*"
                    />
                  </div>
                  <div>
                    <label htmlFor="topic" className="block text-xs font-medium text-gray-600 mb-1">
                      Topic *
                    </label>
                    <select
                      id="topic"
                      required
                      value={formData.topic}
                      onChange={(e) => setFormData(prev => ({ ...prev, topic: e.target.value, partition: 0 }))}
                      className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                    >
                      <option value="">-- 选择Topic --</option>
                      {topics.map((topic) => {
                        const topicName = topic.name || topic.topicName || '';
                        return (
                          <option key={topicName} value={topicName}>
                            {topicName}
                          </option>
                        );
                      })}
                    </select>
                  </div>
                  <div>
                    <label htmlFor="partition" className="block text-xs font-medium text-gray-600 mb-1">
                      分区
                    </label>
                    <select
                      id="partition"
                      value={formData.partition}
                      onChange={(e) => setFormData(prev => ({ ...prev, partition: parseInt(e.target.value) || 0 }))}
                      className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                      disabled={!formData.topic}
                    >
                      {Array.from({ length: getPartitionCount() }, (_, i) => (
                        <option key={i} value={i}>
                          分区 {i}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <div className="flex gap-2">
                  <Button
                    type="submit"
                    disabled={formLoading}
                    className="bg-green-600 hover:bg-green-700 text-white"
                  >
                    {formLoading ? '创建中...' : '创建规则'}
                  </Button>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => setShowCreateForm(false)}
                  >
                    取消
                  </Button>
                </div>
              </form>
            </CardContent>
          </Card>
        )}

        {/* 过滤器 */}
        <Card className="shadow-sm mb-6">
          <CardContent className="pt-6">
            <div className="flex items-center gap-4">
              <Filter className="h-5 w-5 text-gray-400" />
              <div className="flex-1">
                <label htmlFor="filter" className="text-sm font-medium text-gray-700 mr-3">
                  筛选消费组：
                </label>
                <select
                  id="filter"
                  value={filterGroupId}
                  onChange={(e) => setFilterGroupId(e.target.value)}
                  className="px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                >
                  <option value="">-- 显示所有消费组 --</option>
                  {allGroupIds.map((groupId) => (
                    <option key={groupId} value={groupId}>
                      {groupId}
                    </option>
                  ))}
                </select>
              </div>
              {filterGroupId && (
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setFilterGroupId('')}
                >
                  清除筛选
                </Button>
              )}
            </div>
          </CardContent>
        </Card>

        {/* 使用说明 */}
        <div className="mb-6 p-4 bg-blue-50 rounded-lg border border-blue-100">
          <div className="flex items-start">
            <Info className="h-5 w-5 text-blue-500 mt-0.5 mr-2 flex-shrink-0" />
            <div className="text-sm text-blue-700">
              <p className="font-medium mb-1">使用说明</p>
              <ul className="list-disc list-inside space-y-1 text-blue-600">
                <li><strong>消费者ID模式</strong>: 支持精确匹配或通配符匹配（如 consumer-*, *-worker）</li>
                <li>手动分配规则优先级高于自动分区分配策略</li>
                <li>删除规则后，点击对应消费组的"立即应用"按钮即可恢复自动分配</li>
                <li>新建规则后，点击"立即应用"按钮可立即生效，或等待 10 秒自动生效</li>
              </ul>
            </div>
          </div>
        </div>

        {/* 按消费组显示规则 */}
        {displayGroupIds.length === 0 ? (
          <Card className="shadow-sm">
            <CardContent className="py-12 text-center text-gray-500">
              <div className="text-lg mb-2">暂无消费组数据</div>
              <p className="text-sm">请先创建消费组，然后刷新此页面</p>
            </CardContent>
          </Card>
        ) : (
          <div className="space-y-6">
            {displayGroupIds.map((groupId) => {
              const assignments = groupedAssignments[groupId] || [];
              const isRebalancing = rebalancing[groupId] || false;

              return (
                <Card key={groupId} className="shadow-sm">
                  <CardHeader className="border-b border-gray-100">
                    <div className="flex justify-between items-center">
                      <div>
                        <CardTitle className="text-lg flex items-center gap-2">
                          {groupId}
                          <span className="text-sm font-normal text-gray-500">
                            ({assignments.length} 条规则)
                          </span>
                        </CardTitle>
                      </div>
                      <Button
                        size="sm"
                        disabled={isRebalancing}
                        onClick={() => handleTriggerRebalance(groupId)}
                        className="bg-blue-600 hover:bg-blue-700 text-white"
                      >
                        <RefreshCw className={`h-4 w-4 mr-1 ${isRebalancing ? 'animate-spin' : ''}`} />
                        {isRebalancing ? '重新均衡中...' : '立即应用更改'}
                      </Button>
                    </div>
                  </CardHeader>
                  <CardContent className="pt-6">
                    <div className="overflow-x-auto">
                      <table className="w-full">
                        <thead>
                          <tr className="bg-gray-50">
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              ID
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              消费者ID模式
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              Topic
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              分区
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              创建时间
                            </th>
                            <th className="px-4 py-2 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                              操作
                            </th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                          {assignments.length === 0 ? (
                            <tr>
                              <td colSpan={6} className="px-4 py-8 text-center text-gray-500">
                                暂无手动分配规则，使用自动分区分配策略
                              </td>
                            </tr>
                          ) : (
                            assignments.map((assignment) => (
                              <tr key={assignment.id} className="hover:bg-gray-50 transition-colors">
                                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-600">
                                  {assignment.id}
                                </td>
                                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-900 font-mono">
                                  {assignment.consumer_id_pattern}
                                </td>
                                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-600">
                                  {assignment.topic}
                                </td>
                                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-600">
                                  {assignment.partition}
                                </td>
                                <td className="px-4 py-2 whitespace-nowrap text-sm text-gray-500">
                                  {new Date(assignment.created_at).toLocaleString('zh-CN')}
                                </td>
                                <td className="px-4 py-2 whitespace-nowrap">
                                  <Button
                                    size="sm"
                                    variant="outline"
                                    className="text-red-600 hover:text-red-800"
                                    onClick={() => handleDelete(assignment.id)}
                                  >
                                    <Trash2 className="h-4 w-4" />
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
              );
            })}
          </div>
        )}

        {/* 调试信息（仅开发模式） */}
        {process.env.NODE_ENV === 'development' && (
          <div className="mt-8 p-4 bg-gray-100 rounded-md text-xs font-mono">
            <div className="font-bold mb-2">调试信息：</div>
            <div>消费组数量: {consumerGroups.length}</div>
            <div>Topic 数量: {topics.length}</div>
            <div>分配规则总数: {allAssignments.length}</div>
            <div>加载状态: {loading ? '加载中' : '已加载'}</div>
            <div>错误信息: {error || '无'}</div>
            <div>当前筛选: {filterGroupId || '所有消费组'}</div>
            <div>API 基础 URL: {apiConfig.baseURL}</div>
            <div>API 完整路径: {apiConfig.fullURL}</div>
          </div>
        )}
      </div>
    </div>
  );
}
