import type { Metadata } from "next";
import { Suspense } from "react";
import "./globals.css";
import AuthGuard from "@/components/auth-guard";
import TitleManager from "@/components/title-manager";

export const metadata: Metadata = {
  title: "Message Console",
  description: "基于数据库的消息队列监控和管理界面",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="zh-CN">
      <body className="antialiased">
        <Suspense fallback={null}>
          <TitleManager />
        </Suspense>
        <AuthGuard>{children}</AuthGuard>
      </body>
    </html>
  );
}
