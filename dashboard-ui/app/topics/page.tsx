'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { TopicMetrics } from '@/lib/types';
import { formatNumber, formatBytes, formatTimestamp } from '@/lib/utils';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { 
  ArrowLeft, 
  Plus, 
  RefreshCw, 
  Eye, 
  Settings, 
  Database, 
  MessageSquare, 
  HardDrive,
  ExternalLink
} from 'lucide-react';
import Link from 'next/link';

export default function TopicsManagerPage() {
  const router = useRouter();
  
  const [topics, setTopics] = useState<TopicMetrics[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // 加载Topics列表
  const loadTopics = async () => {
    try {
      setLoading(true);
      const topicsData = await DBMQAPIClient.getTopics();
      setTopics(topicsData);
      setError(null);
    } catch (err) {
      console.error('Failed to load topics:', err);
      setError(err instanceof Error ? err.message : '加载Topics失败');
    } finally {
      setLoading(false);
    }
  };

  const handleViewTopic = (topicName: string) => {
    router.push(`/topics/${encodeURIComponent(topicName)}`);
  };

  useEffect(() => {
    loadTopics();
  }, []);

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <div className="text-center">
          <RefreshCw className="h-8 w-8 animate-spin mx-auto mb-4 text-blue-600" />
          <p className="text-gray-600">加载Topics中...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* 头部 */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center">
              <Link 
                href="/" 
                className="inline-flex items-center text-blue-600 hover:text-blue-800 mr-4"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                返回仪表板
              </Link>
              <h1 className="text-3xl font-bold text-gray-900">Topic 管理</h1>
            </div>
            <div className="flex items-center space-x-4">
              <Button onClick={loadTopics} size="sm" disabled={loading} variant="outline">
                <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              </Button>
              <Link href="/topics/create">
                <Button variant="primary">
                  <Plus className="h-4 w-4 mr-2" />
                  创建新 Topic
                </Button>
              </Link>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {error && (
          <div className="mb-6 bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded">
            <p className="font-bold">错误</p>
            <p>{error}</p>
          </div>
        )}

        <Card>
          <CardHeader>
            <CardTitle>Topics 列表</CardTitle>
          </CardHeader>
          <CardContent>
            {topics.length === 0 ? (
              <div className="text-center py-12">
                <Database className="h-12 w-12 mx-auto mb-4 text-gray-400" />
                <p className="text-gray-500 text-lg">暂无Topics</p>
                <p className="text-gray-400 mt-2">创建第一个Topic开始使用DBMQ</p>
                <Link href="/topics/create" className="mt-4 inline-block">
                  <Button variant="primary">
                    <Plus className="h-4 w-4 mr-2" />
                    创建新 Topic
                  </Button>
                </Link>
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Topic 名称
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        分区数
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        消息数
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        存储大小
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        状态
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        创建时间
                      </th>
                      <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                        操作
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {topics.map((topic) => (
                      <tr key={topic.name || topic.topicName} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <Database className="h-5 w-5 text-blue-500 mr-3" />
                            <div>
                              <div className="text-sm font-medium text-gray-900">
                                {topic.name || topic.topicName}
                              </div>
                              {topic.description && (
                                <div className="text-sm text-gray-500">
                                  {topic.description}
                                </div>
                              )}
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <Database className="h-4 w-4 text-gray-400 mr-2" />
                            <span className="text-sm text-gray-900">
                              {topic.partitionCount || topic.partitions?.length || 0}
                            </span>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <MessageSquare className="h-4 w-4 text-gray-400 mr-2" />
                            <span className="text-sm text-gray-900">
                              {formatNumber(topic.messageCount || 0)}
                            </span>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="flex items-center">
                            <HardDrive className="h-4 w-4 text-gray-400 mr-2" />
                            <span className="text-sm text-gray-900">
                              {formatBytes((topic as TopicMetrics & { sizeBytes?: number })?.sizeBytes || 0)}
                            </span>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap">
                          <Badge variant="success">
                            活跃
                          </Badge>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {topic.createdAt ? formatTimestamp(topic.createdAt) : '--'}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                          <div className="flex items-center justify-end space-x-2">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => handleViewTopic(topic.name || topic.topicName || '')}
                            >
                              <Eye className="h-4 w-4 mr-1" />
                              查看
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => {
                                // 可以在这里添加配置功能
                                console.log('Configure topic:', topic.name || topic.topicName);
                              }}
                            >
                              <Settings className="h-4 w-4 mr-1" />
                              配置
                            </Button>
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>

        {/* 快速操作卡片 */}
        <div className="mt-8 grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card className="cursor-pointer hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <Link href="/topics/create" className="block">
                <div className="flex items-center">
                  <Plus className="h-8 w-8 text-blue-600 mr-4" />
                  <div>
                    <h3 className="text-lg font-medium text-gray-900">创建 Topic</h3>
                    <p className="text-sm text-gray-500">创建新的消息队列Topic</p>
                  </div>
                </div>
              </Link>
            </CardContent>
          </Card>

          <Card className="cursor-pointer hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <Link href="/producer" className="block">
                <div className="flex items-center">
                  <MessageSquare className="h-8 w-8 text-green-600 mr-4" />
                  <div>
                    <h3 className="text-lg font-medium text-gray-900">消息生产器</h3>
                    <p className="text-sm text-gray-500">发送消息到Topic</p>
                  </div>
                </div>
              </Link>
            </CardContent>
          </Card>

          <Card className="cursor-pointer hover:shadow-lg transition-shadow">
            <CardContent className="p-6">
              <Link href="/" className="block">
                <div className="flex items-center">
                  <ExternalLink className="h-8 w-8 text-purple-600 mr-4" />
                  <div>
                    <h3 className="text-lg font-medium text-gray-900">仪表板</h3>
                    <p className="text-sm text-gray-500">查看系统概览和统计</p>
                  </div>
                </div>
              </Link>
            </CardContent>
          </Card>
        </div>
      </main>
    </div>
  );
} 