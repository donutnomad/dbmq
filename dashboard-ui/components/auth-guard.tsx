'use client';

import { useState, useEffect, useCallback } from 'react';
import { getAccessToken, setAccessToken } from '@/lib/api';
import { apiConfig } from '@/config/api.config';

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<'checking' | 'login' | 'ok'>('checking');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');

  const checkAuth = useCallback(async () => {
    try {
      const token = getAccessToken();
      const headers: Record<string, string> = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch(`${apiConfig.fullURL}/health`, { headers });
      if (res.ok) {
        setState('ok');
      } else if (res.status === 401) {
        setState('login');
      } else {
        // 服务没有开启 token 校验，直接放行
        setState('ok');
      }
    } catch {
      // 网络错误，仍然放行让页面自己处理错误
      setState('ok');
    }
  }, []);

  useEffect(() => {
    checkAuth();
  }, [checkAuth]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    try {
      const res = await fetch(`${apiConfig.fullURL}/health`, {
        headers: { 'Authorization': `Bearer ${password}` },
      });
      if (res.ok) {
        setAccessToken(password);
        setState('ok');
      } else {
        setError('密码错误');
      }
    } catch {
      setError('连接失败');
    }
  };

  if (state === 'checking') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <p className="text-gray-500">验证中...</p>
      </div>
    );
  }

  if (state === 'login') {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <form onSubmit={handleSubmit} className="bg-white p-8 rounded-lg shadow-md w-80">
          <h2 className="text-xl font-semibold text-gray-800 mb-6 text-center">DBMQ Dashboard</h2>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="请输入访问密码"
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 mb-4"
            autoFocus
          />
          {error && <p className="text-red-500 text-sm mb-4">{error}</p>}
          <button
            type="submit"
            className="w-full bg-blue-600 text-white py-2 rounded-md hover:bg-blue-700 transition-colors"
          >
            登录
          </button>
        </form>
      </div>
    );
  }

  return <>{children}</>;
}
