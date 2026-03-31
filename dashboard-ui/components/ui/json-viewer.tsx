'use client';

import React from 'react';
import JsonView from '@uiw/react-json-view';
import { cn } from '@/lib/utils';

interface JsonViewerProps {
  data: string | object;
  collapsed?: number | boolean;
  theme?: 'light' | 'dark';
  className?: string;
}

export function JsonViewer({
  data,
  collapsed = false,
  theme = 'light',
  className
}: JsonViewerProps) {
  const [jsonData, setJsonData] = React.useState<any>(null);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    try {
      if (typeof data === 'string') {
        setJsonData(JSON.parse(data));
        setError(null);
      } else {
        setJsonData(data);
        setError(null);
      }
    } catch (err) {
      setError('无效的 JSON 格式');
      setJsonData(null);
    }
  }, [data]);

  if (error) {
    return (
      <div className={cn(
        "p-4 bg-red-50 border border-red-200 rounded-md",
        className
      )}>
        <p className="text-red-700 text-sm">{error}</p>
        <pre className="mt-2 text-xs text-gray-600 overflow-auto">
          {typeof data === 'string' ? data : JSON.stringify(data)}
        </pre>
      </div>
    );
  }

  if (!jsonData) {
    return (
      <div className={cn("p-4 bg-gray-100 rounded-md", className)}>
        <p className="text-gray-500 text-sm">加载中...</p>
      </div>
    );
  }

  return (
    <div className={cn(
      "json-viewer-container rounded-md overflow-auto",
      theme === 'dark' ? 'bg-gray-900' : 'bg-white',
      className
    )}>
      <JsonView
        value={jsonData}
        collapsed={collapsed}
        style={{
          backgroundColor: theme === 'dark' ? '#1e293b' : '#ffffff',
          padding: '1rem',
          fontSize: '0.875rem',
          fontFamily: 'ui-monospace, monospace',
        }}
        displayDataTypes={false}
        displayObjectSize={true}
        enableClipboard={true}
      />
    </div>
  );
}
