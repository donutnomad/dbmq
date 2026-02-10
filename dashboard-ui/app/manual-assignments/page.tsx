'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { ManualAssignment, CreateManualAssignmentRequest, ConsumerGroupMetrics, TopicMetrics } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ArrowLeft, RefreshCw, Plus, Trash2, Info } from 'lucide-react';
import Link from 'next/link';

export default function ManualAssignmentsPage() {
  const router = useRouter();
  const [loading, setLoading] = useState(true);
  const [consumerGroups, setConsumerGroups] = useState<ConsumerGroupMetrics[]>([]);
  const [topics, setTopics] = useState<TopicMetrics[]>([]);
  const [selectedGroupId, setSelectedGroupId] = useState<string>('');
  const [assignments, setAssignments] = useState<ManualAssignment[]>([]);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [formLoading, setFormLoading] = useState(false);
  const [formData, setFormData] = useState<CreateManualAssignmentRequest>({
    group_id: '',
    consumer_id_pattern: '',
    topic: '',
    partition: 0,
  });

  // 加载消费组和Topic列表
  useEffect(() => {
    const loadInitialData = async () => {
      try {
        const [groupsData, topicsData] = await Promise.all([
          DBMQAPIClient.getConsumerGroups(),
          DBMQAPIClient.getTopics(true),
        ]);
        setConsumerGroups(groupsData);
        setTopics(topicsData);
      } catch (error) {
        console.error('Failed to load initial data:', error);
      } finally {
        setLoading(false);
      }
    };
    loadInitialData();
  }, []);

  // 当选择消费组时，加载分配规则
  useEffect(() => {
    const loadData = async () => {
      if (!selectedGroupId) {
        setAssignments([]);
        return;
      }
      try {
        const data = await DBMQAPIClient.getManualAssignments(selectedGroupId);
        setAssignments(data);
      } catch (error) {
        console.error('Failed to load assignments:', error);
        setAssignments([]);
      }
      setFormData(prev => ({ ...prev, group_id: selectedGroupId }));
    };
    loadData();
  }, [selectedGroupId]);

  const loadAssignments = async () => {
    if (!selectedGroupId) return;
    try {
      const data = await DBMQAPIClient.getManualAssignments(selectedGroupId);
      setAssignments(data);
    } catch (error) {
      console.error('Failed to load assignments:', error);
      setAssignments([]);
    }
  };

  const handleCreateSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.consumer_id_pattern.trim() || !formData.topic) {
      alert('请填写所有必填字段');
      return;
    }

    setFormLoading(true);
    try {
      await DBMQAPIClient.createManualAssignment(formData);
      setShowCreateForm(false);
      setFormData({
        group_id: selectedGroupId,
        consumer_id_pattern: '',
        topic: '',
        partition: 0,
      });
      await loadAssignments();
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
      await loadAssignments();
    } catch (error) {
      console.error('Failed to delete assignment:', error);
      alert('删除分配规则失败: ' + (error instanceof Error ? error.message : '未知错误'));
    }
  };

  // 获取选中Topic的分区数
  const getPartitionCount = (): number => {
    const topic = topics.find(t => (t.name || t.topicName) === formData.topic);
    return topic?.partitionCount || topic?.partitions?.length || 1;
  };

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
            <p className="text-gray-600 mt-1">为消费组配置手动分区分配规则</p>
          </div>
          <Button onClick={loadAssignments} size="sm" variant="outline" disabled={!selectedGroupId}>
            <RefreshCw className="h-4 w-4 mr-1" />
            刷新
          </Button>
        </div>

        {/* 消费组选择器 */}
        <Card className="shadow-sm mb-6">
          <CardHeader className="border-b border-gray-100">
            <CardTitle className="text-lg">选择消费组</CardTitle>
          </CardHeader>
          <CardContent className="pt-6">
            <select
              value={selectedGroupId}
              onChange={(e) => setSelectedGroupId(e.target.value)}
              className="w-full max-w-md px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
            >
              <option value="">-- 请选择消费组 --</option>
              {consumerGroups.map((group) => {
                const groupId = group.groupId || group.name || '';
                return (
                  <option key={groupId} value={groupId}>
                    {groupId}
                  </option>
                );
              })}
            </select>
          </CardContent>
        </Card>

        {/* 分配规则卡片 */}
        {selectedGroupId && (
          <Card className="shadow-sm">
            <CardHeader className="border-b border-gray-100">
              <div className="flex justify-between items-center">
                <CardTitle className="text-lg">
                  分配规则列表 - {selectedGroupId}
                </CardTitle>
                <Button
                  size="sm"
                  variant="outline"
                  className="text-green-600 hover:text-green-800"
                  onClick={() => setShowCreateForm(!showCreateForm)}
                >
                  <Plus className="h-4 w-4 mr-1" />
                  {showCreateForm ? '取消添加' : '添加规则'}
                </Button>
              </div>
            </CardHeader>
            <CardContent className="pt-6">
              {/* 创建表单 */}
              {showCreateForm && (
                <form onSubmit={handleCreateSubmit} className="mb-6 p-4 bg-gray-50 rounded-lg border border-gray-200">
                  <h3 className="text-sm font-medium text-gray-700 mb-4">添加新规则</h3>
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
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
                    <div className="flex items-end">
                      <Button
                        type="submit"
                        variant="primary"
                        loading={formLoading}
                        className="bg-green-600 hover:bg-green-700 text-white"
                      >
                        创建规则
                      </Button>
                    </div>
                  </div>
                </form>
              )}

              {/* 规则表格 */}
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
                          暂无分配规则
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

              {/* 使用说明 */}
              <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-100">
                <div className="flex items-start">
                  <Info className="h-5 w-5 text-blue-500 mt-0.5 mr-2 flex-shrink-0" />
                  <div className="text-sm text-blue-700">
                    <p className="font-medium mb-1">使用说明</p>
                    <ul className="list-disc list-inside space-y-1 text-blue-600">
                      <li><strong>消费者ID模式</strong>: 支持精确匹配或通配符匹配（如 consumer-*, *-worker）</li>
                      <li>手动分配规则优先级高于自动分区分配策略</li>
                      <li>删除规则后，下次重新均衡时将使用自动分配策略</li>
                    </ul>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
