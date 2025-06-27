'use client';

import { useState, useEffect } from 'react';
import { PartitionStats, PartitionLag } from '@/lib/types';
import { formatNumber, formatBytes, formatTimestamp } from '@/lib/utils';
import { Card, CardContent, CardHeader, CardTitle } from './card';
import { Badge } from './badge';
import { Progress } from './progress';
import { 
  Database, 
  MessageSquare, 
  HardDrive, 
  Hash,
  TrendingUp,
  TrendingDown,
  Activity,
  Clock
} from 'lucide-react';

interface PartitionStatsCardProps {
  partitionStats: PartitionStats;
  partitionLag?: PartitionLag;
  className?: string;
}

export function PartitionStatsCard({ partitionStats, partitionLag, className }: PartitionStatsCardProps) {
  const [progressPercentage, setProgressPercentage] = useState(0);

  useEffect(() => {
    // 计算消费进度
    if (partitionLag && partitionLag.consumedPercentage !== undefined) {
      setProgressPercentage(partitionLag.consumedPercentage);
    }
  }, [partitionLag]);

  return (
    <Card className={className}>
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <Database className="h-4 w-4" />
          分区 {partitionStats.partition}
          {partitionLag && (
            <Badge variant={partitionLag.lag && partitionLag.lag > 0 ? "error" : "default"}>
              {partitionLag.lag && partitionLag.lag > 0 ? `延迟 ${partitionLag.lag}` : '已同步'}
            </Badge>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* 基本统计信息 */}
        <div className="grid grid-cols-2 gap-3 text-sm">
          <div className="flex items-center gap-2">
            <Hash className="h-3 w-3 text-muted-foreground" />
            <span className="text-muted-foreground">消息总数:</span>
            <span className="font-medium">{formatNumber(partitionStats.messageCount)}</span>
          </div>
          <div className="flex items-center gap-2">
            <HardDrive className="h-3 w-3 text-muted-foreground" />
            <span className="text-muted-foreground">大小:</span>
            <span className="font-medium">{formatBytes(partitionStats.sizeBytes)}</span>
          </div>
        </div>

        {/* ID 范围信息 */}
        <div className="space-y-2">
          <div className="text-xs text-muted-foreground">消息ID范围</div>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div>
              <span className="text-muted-foreground">首个ID:</span>
              <span className="ml-2 font-mono">
                {partitionStats.firstMessageId === -1 ? '无' : partitionStats.firstMessageId}
              </span>
            </div>
            <div>
              <span className="text-muted-foreground">最新ID:</span>
              <span className="ml-2 font-mono">
                {partitionStats.lastMessageId === -1 ? '无' : partitionStats.lastMessageId}
              </span>
            </div>
          </div>
        </div>

        {/* 消费进度信息 */}
        {partitionLag && (
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">消费进度</span>
              <span className="font-medium">
                {progressPercentage.toFixed(1)}%
              </span>
            </div>
            <Progress value={progressPercentage} className="h-2" />
            
            <div className="grid grid-cols-2 gap-3 text-sm">
              <div className="flex items-center gap-2">
                <TrendingUp className="h-3 w-3 text-green-500" />
                <span className="text-muted-foreground">已消费:</span>
                <span className="font-medium text-green-600">
                  {formatNumber(partitionLag.consumedMessages || 0)}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <TrendingDown className="h-3 w-3 text-orange-500" />
                <span className="text-muted-foreground">剩余:</span>
                <span className="font-medium text-orange-600">
                  {formatNumber(partitionLag.remainingMessages || 0)}
                </span>
              </div>
            </div>

            {/* 当前消费位置 */}
            <div className="text-sm">
              <span className="text-muted-foreground">当前位置:</span>
              <span className="ml-2 font-mono">
                ID {partitionLag.currentOffset || 0}
              </span>
            </div>
          </div>
        )}

        {/* 时间戳信息 */}
        {(partitionStats.createdAt || partitionStats.updatedAt) && (
          <div className="space-y-1 text-xs text-muted-foreground border-t pt-2">
            {partitionStats.createdAt && (
              <div className="flex items-center gap-2">
                <Clock className="h-3 w-3" />
                <span>首条消息: {formatTimestamp(partitionStats.createdAt)}</span>
              </div>
            )}
            {partitionStats.updatedAt && (
              <div className="flex items-center gap-2">
                <Activity className="h-3 w-3" />
                <span>最新消息: {formatTimestamp(partitionStats.updatedAt)}</span>
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

interface PartitionStatsGridProps {
  partitionStats: PartitionStats[];
  partitionLags?: PartitionLag[];
  className?: string;
}

export function PartitionStatsGrid({ partitionStats, partitionLags = [], className }: PartitionStatsGridProps) {
  return (
    <div className={`grid gap-4 ${className}`}>
      {partitionStats.map((stats) => {
        const lag = partitionLags.find(l => l.partition === stats.partition);
        return (
          <PartitionStatsCard
            key={`${stats.topic}-${stats.partition}`}
            partitionStats={stats}
            partitionLag={lag}
          />
        );
      })}
    </div>
  );
} 