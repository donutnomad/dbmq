import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DBMQ 监控仪表板",
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
        {children}
      </body>
    </html>
  );
}
