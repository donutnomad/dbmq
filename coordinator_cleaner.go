package dbmq

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"time"

	"github.com/donutnomad/dbmq/internal/types"
	"github.com/donutnomad/dbmq/logger"
	"github.com/donutnomad/gt"
)

// 清理过期消息
type cleanerTask struct {
	cfg *CoordinatorConfig
	repos
}

func newCleanerTask(cfg *CoordinatorConfig, repos repos) *cleanerTask {
	return &cleanerTask{cfg: cfg, repos: repos}
}

func (c *cleanerTask) logger() *slog.Logger {
	return logger.GetLogger().With("component", "coordinator:cleaner")
}

func (c *cleanerTask) Name() string { return "coordinator cleaner" }

func (c *cleanerTask) Start(ctx context.Context) error {
	return gt.TickRun(ctx, gt.CRON, c.cfg.RetentionCheckInterval, 0, 0, func(ctx context.Context) *gt.TickOptions {
		c.logger().Debug("[LEADER] Starting message cleanup...")
		c.clean(ctx)
		return nil
	})
}

func (c *cleanerTask) clean(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute) // 清理的慷慨超时时间
	defer cancel()

	log := c.logger()
	log.Debug("Starting message retention cleanup cycle.")
	startTime := time.Now()

	// 1. 获取Topic的时长保留策略
	allTopics, err := c.topicRepo.GetAll(ctx)
	if err != nil {
		log.Error("Cleanup failed to get topics", "error", err)
		return
	}

	// 2. Calculate global low watermark for all consumed partitions
	watermarks, err := c.progressRepo.GetLowWatermarks(ctx)
	if err != nil {
		log.Error("Cleanup failed to get low watermarks", "error", err)
		return
	}
	log.Debug(fmt.Sprintf("Found %d consumed partitions with a low watermark.", len(watermarks)))

	ite := iter.Seq2[types.PartitionInfo, time.Duration](func(yield func(info types.PartitionInfo, v time.Duration) bool) {
		for _, t := range allTopics {
			var dur time.Duration = 0
			retentionMs, _ := t.GetConfig("retention_ms")
			if retentionMs > 0 {
				dur = time.Duration(retentionMs) * time.Millisecond
			} else {
				dur = c.cfg.DefaultRetentionAge
			}
			for i := uint(0); i < t.PartitionCount; i++ {
				p := types.PartitionInfo{Topic: t.Name, Partition: i}
				if !yield(p, dur) {
					return
				}
			}
		}
	})

	var totalDeletedCount int64
	for p, dur := range ite {
		partitionTotalDeleted := c.delete(ctx, watermarks, p, time.Now().Add(-dur), 1000, log)
		if partitionTotalDeleted == 0 {
			continue
		}
		totalDeletedCount += partitionTotalDeleted
		log.Debug(fmt.Sprintf("Cleaned up %d messages from partition %v", partitionTotalDeleted, p))
	}

	log.Debug(fmt.Sprintf("Finished message retention cleanup cycle in %v. Total messages deleted: %d", time.Since(startTime), totalDeletedCount))
}

func (c *cleanerTask) delete(ctx context.Context, watermarks map[types.PartitionInfo]int64, p types.PartitionInfo, retentionDate time.Time, cleanupBatchSize int, log *slog.Logger) int64 {
	var partitionTotalDeleted int64

	// Loop to delete in batches until no more rows are affected
	for {
		if ctx.Err() != nil {
			log.Debug("Cleanup cancelled during batch processing for partition", "partition", p)
			break
		}

		var deletedCount int64
		var err error

		if lowWatermark, ok := watermarks[p]; ok {
			// This partition is consumed, so use the low watermark
			deletedCount, err = c.messageRepo.DeleteConsumed(ctx, p.Topic, p.Partition, lowWatermark, retentionDate, cleanupBatchSize)
		} else {
			// This partition is not in the watermark map, meaning no group has ever committed an offset for it.
			// We can only clean it up based on time.
			deletedCount, err = c.messageRepo.DeleteExpired(ctx, p.Topic, p.Partition, retentionDate, cleanupBatchSize)
		}
		if err != nil {
			log.Error("Failed to clean partition", "partition", p, "error", err)
			break
		}
		partitionTotalDeleted += deletedCount

		// If we deleted fewer rows than the batch size, we are done with this partition.
		if deletedCount < int64(cleanupBatchSize) {
			break
		}

		gt.SleepCtx(ctx, 100*time.Millisecond)
	}

	return partitionTotalDeleted
}
