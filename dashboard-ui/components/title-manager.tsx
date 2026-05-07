'use client';

import { useMemo } from 'react';
import { usePathname, useSearchParams } from 'next/navigation';
import { useDocumentTitle } from '@/lib/use-document-title';

const PAGE_TITLES: Record<string, string> = {
  '/': 'Message Console',
  '/clusters': '集群管理',
  '/consumers': '消费者',
  '/producer': '消息生产器',
  '/topics/create': '创建新 Topic',
};

function safeDecodeURIComponent(value: string): string {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function titleForPath(pathname: string, searchParams: URLSearchParams): string {
  if (pathname === '/topics') {
    const topicName = searchParams.get('name');
    return topicName ? `${safeDecodeURIComponent(topicName)} - Message Console` : '请选择一个 Topic';
  }

  if (pathname === '/consumer-groups') {
    const groupId = searchParams.get('id');
    return groupId ? `${safeDecodeURIComponent(groupId)} - Message Console` : '消费组详情';
  }

  return PAGE_TITLES[pathname] || 'Message Console';
}

export default function TitleManager() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const search = searchParams.toString();

  const title = useMemo(
    () => titleForPath(pathname, new URLSearchParams(search)),
    [pathname, search]
  );

  useDocumentTitle(title);
  return null;
}
