'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { DBMQAPIClient } from '@/lib/api';
import { NewTopicRequest } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { ArrowLeft } from 'lucide-react';
import Link from 'next/link';

export default function CreateTopicPage() {
  const router = useRouter();
  const [loading, setLoading] = useState(false);
  const [formData, setFormData] = useState<NewTopicRequest>({
    name: '',
    numPartitions: 1,
  });

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!formData.name.trim()) {
      alert('请输入Topic名称');
      return;
    }

    setLoading(true);
    try {
      await DBMQAPIClient.createTopic(formData);
      alert('Topic创建成功！');
      router.push('/');
    } catch (error) {
      console.error('Failed to create topic:', error);
      alert('创建Topic失败: ' + (error instanceof Error ? error.message : '未知错误'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-8">
      <div className="page-shell">
        {/* 头部 */}
        <div className="mb-8">
          <Link 
            href="/" 
            className="inline-flex items-center text-gray-600 hover:text-gray-800 mb-4 transition-colors"
          >
            <ArrowLeft className="h-4 w-4 mr-2" />
            返回仪表板
          </Link>
          <h1 className="text-2xl font-semibold text-gray-900">创建新 Topic</h1>
          <p className="text-gray-600 mt-1">配置并创建一个新的消息队列Topic</p>
        </div>

        {/* 表单 */}
        <Card className="shadow-sm">
          <CardHeader className="border-b border-gray-100">
            <CardTitle className="text-lg">Topic 配置</CardTitle>
          </CardHeader>
          <CardContent className="pt-6">
            <form onSubmit={handleSubmit} className="space-y-6">
              <div>
                <label htmlFor="name" className="block text-sm font-medium text-gray-700 mb-1">
                  Topic 名称 *
                </label>
                <input
                  type="text"
                  id="name"
                  required
                  value={formData.name}
                  onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                  placeholder="输入Topic名称"
                />
              </div>

              <div>
                <label htmlFor="partitions" className="block text-sm font-medium text-gray-700 mb-1">
                  分区数
                </label>
                <input
                  type="number"
                  id="partitions"
                  min="1"
                  max="100"
                  value={formData.numPartitions}
                  onChange={(e) => setFormData(prev => ({ ...prev, numPartitions: parseInt(e.target.value) || 1 }))}
                  className="w-full px-3 py-2 border border-gray-200 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 text-sm"
                />
                <p className="mt-1 text-xs text-gray-500">
                  分区数决定了Topic的并行处理能力，建议根据预期的消息量设置
                </p>
              </div>

              <div className="flex justify-end space-x-4 pt-6 border-t border-gray-100">
                <Link href="/">
                  <Button variant="outline" type="button" className="text-gray-600 hover:text-gray-800">
                    取消
                  </Button>
                </Link>
                <Button 
                  type="submit" 
                  variant="primary" 
                  loading={loading}
                  disabled={!formData.name.trim()}
                  className="bg-blue-600 hover:bg-blue-700 text-white"
                >
                  创建 Topic
                </Button>
              </div>
            </form>
          </CardContent>
        </Card>
      </div>
    </div>
  );
} 