'use client';

import { useState, useEffect, useCallback } from 'react';
import { DBMQAPIClient } from '@/lib/api';
import { TopicMetrics } from '@/lib/types';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { 
  ArrowLeft, 
  Send, 
  CheckCircle, 
  XCircle, 
  BarChart3
} from 'lucide-react';
import Link from 'next/link';

interface ProducerStats {
  success: number;
  error: number;
  total: number;
}

export default function ProducerPage() {
  
  const [topics, setTopics] = useState<TopicMetrics[]>([]);
  const [loading, setLoading] = useState(true);
  const [sending, setSending] = useState(false);
  const [stats, setStats] = useState<ProducerStats>({ success: 0, error: 0, total: 0 });
  const [message, setMessage] = useState('');
  const [messageType, setMessageType] = useState<'success' | 'error' | ''>('');

  // 表单数据
  const [formData, setFormData] = useState({
    topic: '',
    messageKey: '',
    messageValue: '',
    headers: '',
    batchCount: 1,
    interval: 0
  });

  // 加载Topics列表
  const loadTopics = useCallback(async () => {
    try {
      setLoading(true);
      const topicsData = await DBMQAPIClient.getTopics();
      setTopics(topicsData);
    } catch (error) {
      console.error('Failed to load topics:', error);
      showMessage('获取Topics列表失败: ' + (error instanceof Error ? error.message : '未知错误'), 'error');
    } finally {
      setLoading(false);
    }
  }, []);

  // 显示消息
  const showMessage = (text: string, type: 'success' | 'error') => {
    setMessage(text);
    setMessageType(type);
    setTimeout(() => {
      setMessage('');
      setMessageType('');
    }, 5000);
  };

  // 发送消息
  const sendMessage = async () => {
    // 验证表单
    if (!formData.topic || !formData.messageValue) {
      showMessage('请填写必需的字段（Topic和消息内容）', 'error');
      return;
    }

    // 解析headers
    let headers = {};
    if (formData.headers) {
      try {
        headers = JSON.parse(formData.headers);
      } catch {
        showMessage('消息头格式错误，请使用有效的JSON格式', 'error');
        return;
      }
    }

    setSending(true);
    let successCount = 0;
    let errorCount = 0;

    try {
      for (let i = 0; i < formData.batchCount; i++) {
        const messageData = {
          topic: formData.topic,
          key: formData.messageKey || null,
          value: formData.messageValue,
          headers: headers
        };

        try {
          await DBMQAPIClient.sendMessage(formData.topic, messageData);
          successCount++;
        } catch (error) {
          errorCount++;
          console.error('Message send failed:', error);
        }

        // 等待间隔时间
        if (formData.interval > 0 && i < formData.batchCount - 1) {
          await new Promise(resolve => setTimeout(resolve, formData.interval));
        }
      }

      // 更新统计
      setStats(prev => ({
        success: prev.success + successCount,
        error: prev.error + errorCount,
        total: prev.total + successCount + errorCount
      }));

      showMessage(
        `消息发送完成！成功: ${successCount}，失败: ${errorCount}`, 
        errorCount === 0 ? 'success' : 'error'
      );
                   
    } catch (error) {
      showMessage('发送失败: ' + (error instanceof Error ? error.message : '未知错误'), 'error');
    } finally {
      setSending(false);
    }
  };

  // 清空表单
  const clearForm = () => {
    setFormData({
      topic: '',
      messageKey: '',
      messageValue: '',
      headers: '',
      batchCount: 1,
      interval: 0
    });
    setStats({ success: 0, error: 0, total: 0 });
  };

  // 加载示例消息
  const loadSampleMessage = () => {
    setFormData(prev => ({
      ...prev,
      messageKey: `sample-key-${Date.now()}`,
      messageValue: JSON.stringify({
        id: Date.now(),
        message: "这是一个示例消息",
        timestamp: new Date().toISOString(),
        data: {
          user: "用户123",
          action: "示例操作"
        }
      }, null, 2),
      headers: JSON.stringify({
        "source": "dashboard",
        "version": "1.0"
      }, null, 2)
    }));
  };

  useEffect(() => {
    loadTopics();
  }, [loadTopics]);

  return (
    <div className="min-h-screen bg-gray-50">
      {/* 头部 */}
      <header className="bg-white border-b border-gray-200">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div className="flex items-center">
              <Link 
                href="/" 
                className="inline-flex items-center text-blue-600 hover:text-blue-800 mr-4"
              >
                <ArrowLeft className="h-4 w-4 mr-2" />
                返回仪表板
              </Link>
              <div>
                <h1 className="text-3xl font-bold text-gray-900">消息生产器</h1>
                <p className="text-gray-600 mt-1">发送消息到指定Topic</p>
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* 消息提示 */}
        {message && (
          <div className={`mb-6 p-4 rounded-md ${
            messageType === 'success' ? 'bg-green-100 border border-green-400 text-green-700' : 
            'bg-red-100 border border-red-400 text-red-700'
          }`}>
            {message}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* 表单区域 */}
          <div className="lg:col-span-2">
            <Card>
              <CardHeader>
                <CardTitle>消息配置</CardTitle>
              </CardHeader>
              <CardContent>
                <form onSubmit={(e) => { e.preventDefault(); sendMessage(); }} className="space-y-6">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                      <label htmlFor="topic" className="block text-sm font-medium text-gray-700 mb-2">
                        目标 Topic *
                      </label>
                      <select
                        id="topic"
                        required
                        value={formData.topic}
                        onChange={(e) => setFormData(prev => ({ ...prev, topic: e.target.value }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        disabled={loading}
                      >
                        <option value="">选择Topic...</option>
                        {topics.map((topic) => (
                          <option key={topic.name || topic.topicName} value={topic.name || topic.topicName}>
                            {topic.name || topic.topicName}
                          </option>
                        ))}
                      </select>
                      <p className="mt-1 text-sm text-gray-500">选择要发送消息的Topic</p>
                    </div>

                    <div>
                      <label htmlFor="messageKey" className="block text-sm font-medium text-gray-700 mb-2">
                        消息Key
                      </label>
                      <input
                        type="text"
                        id="messageKey"
                        value={formData.messageKey}
                        onChange={(e) => setFormData(prev => ({ ...prev, messageKey: e.target.value }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                        placeholder="可选的消息键"
                      />
                      <p className="mt-1 text-sm text-gray-500">用于分区路由的消息键（可选）</p>
                    </div>
                  </div>

                  <div>
                    <label htmlFor="messageValue" className="block text-sm font-medium text-gray-700 mb-2">
                      消息内容 *
                    </label>
                    <textarea
                      id="messageValue"
                      required
                      rows={6}
                      value={formData.messageValue}
                      onChange={(e) => setFormData(prev => ({ ...prev, messageValue: e.target.value }))}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder="输入消息内容，支持JSON格式..."
                    />
                    <p className="mt-1 text-sm text-gray-500">消息的实际内容</p>
                  </div>

                  <div>
                    <label htmlFor="headers" className="block text-sm font-medium text-gray-700 mb-2">
                      消息头（JSON格式）
                    </label>
                    <textarea
                      id="headers"
                      rows={3}
                      value={formData.headers}
                      onChange={(e) => setFormData(prev => ({ ...prev, headers: e.target.value }))}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      placeholder='{"header1": "value1", "header2": "value2"}'
                    />
                    <p className="mt-1 text-sm text-gray-500">可选的消息头，JSON格式</p>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    <div>
                      <label htmlFor="batchCount" className="block text-sm font-medium text-gray-700 mb-2">
                        批量发送数量
                      </label>
                      <input
                        type="number"
                        id="batchCount"
                        min="1"
                        max="1000"
                        value={formData.batchCount}
                        onChange={(e) => setFormData(prev => ({ ...prev, batchCount: parseInt(e.target.value) || 1 }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      />
                      <p className="mt-1 text-sm text-gray-500">一次性发送的消息数量</p>
                    </div>

                    <div>
                      <label htmlFor="interval" className="block text-sm font-medium text-gray-700 mb-2">
                        发送间隔（毫秒）
                      </label>
                      <input
                        type="number"
                        id="interval"
                        min="0"
                        value={formData.interval}
                        onChange={(e) => setFormData(prev => ({ ...prev, interval: parseInt(e.target.value) || 0 }))}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500"
                      />
                      <p className="mt-1 text-sm text-gray-500">批量发送时消息间的间隔</p>
                    </div>
                  </div>

                  <div className="flex justify-end space-x-4 pt-6">
                    <Button 
                      type="button" 
                      variant="outline"
                      onClick={clearForm}
                    >
                      清空
                    </Button>
                    <Button 
                      type="button" 
                      variant="outline"
                      onClick={loadSampleMessage}
                    >
                      示例消息
                    </Button>
                    <Button 
                      type="submit" 
                      variant="primary" 
                      loading={sending}
                      disabled={!formData.topic || !formData.messageValue}
                    >
                      <Send className="h-4 w-4 mr-2" />
                      发送消息
                    </Button>
                  </div>
                </form>
              </CardContent>
            </Card>
          </div>

          {/* 统计区域 */}
          <div className="lg:col-span-1">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center">
                  <BarChart3 className="h-5 w-5 mr-2" />
                  发送统计
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="bg-green-50 p-4 rounded-lg">
                    <div className="flex items-center">
                      <CheckCircle className="h-8 w-8 text-green-600 mr-3" />
                      <div>
                        <div className="text-2xl font-bold text-green-900">{stats.success}</div>
                        <div className="text-sm text-green-700">成功</div>
                      </div>
                    </div>
                  </div>

                  <div className="bg-red-50 p-4 rounded-lg">
                    <div className="flex items-center">
                      <XCircle className="h-8 w-8 text-red-600 mr-3" />
                      <div>
                        <div className="text-2xl font-bold text-red-900">{stats.error}</div>
                        <div className="text-sm text-red-700">失败</div>
                      </div>
                    </div>
                  </div>

                  <div className="bg-blue-50 p-4 rounded-lg">
                    <div className="flex items-center">
                      <BarChart3 className="h-8 w-8 text-blue-600 mr-3" />
                      <div>
                        <div className="text-2xl font-bold text-blue-900">{stats.total}</div>
                        <div className="text-sm text-blue-700">总计</div>
                      </div>
                    </div>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      </main>
    </div>
  );
} 