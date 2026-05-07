'use client';

import { useEffect } from 'react';

export function useDocumentTitle(title: string) {
  useEffect(() => {
    if (!title) return;

    const applyTitle = () => {
      if (document.title !== title) {
        document.title = title;
      }
    };

    applyTitle();
    const observer = new MutationObserver(applyTitle);
    observer.observe(document.head, {
      childList: true,
      subtree: true,
      characterData: true,
    });
    const timer = window.setInterval(applyTitle, 2000);

    window.addEventListener('focus', applyTitle);
    window.addEventListener('pageshow', applyTitle);
    document.addEventListener('visibilitychange', applyTitle);

    return () => {
      observer.disconnect();
      window.clearInterval(timer);
      window.removeEventListener('focus', applyTitle);
      window.removeEventListener('pageshow', applyTitle);
      document.removeEventListener('visibilitychange', applyTitle);
    };
  }, [title]);
}
