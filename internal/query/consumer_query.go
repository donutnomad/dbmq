package query

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
	"github.com/samber/lo"
	"gorm.io/gorm"
)

// ConsumerQuery 消费组查询接口
type ConsumerQuery interface {
	// GetSubscribedTopics 获取消费组订阅的 Topic 列表
	GetSubscribedTopics(ctx context.Context, groupID string) ([]string, error)
	// GetProgressRecords 获取消费组的消费进度记录 (返回 PO - 保留向后兼容)
	GetProgressRecords(ctx context.Context, groupID string) ([]consumerprogressrepo.ProgressPO, error)
	// GetProgressRecordsDTO 获取消费进度记录 (返回 DTO)
	GetProgressRecordsDTO(ctx context.Context, groupID string) ([]ProgressRecord, error)
	// GetProgressRecord 获取消费组指定分区的消费进度
	GetProgressRecord(ctx context.Context, groupID string, topic string, partition uint) (*consumerprogressrepo.ProgressPO, error)
	// GetPartitionLagStats 获取分区延迟统计
	GetPartitionLagStats(ctx context.Context, topic string, partition uint, currentOffset int64, watermark int64) (*PartitionLag, error)
	// GetConsumerGroupExtended 获取消费组扩展信息
	GetConsumerGroupExtended(ctx context.Context, groupID string) (*ConsumerGroupExtended, error)
	// GetConsumerGroupMetrics 获取消费组完整监控指标
	GetConsumerGroupMetrics(ctx context.Context, groupID string) (*ConsumerGroupMetrics, error)
	// GetAllConsumerGroupsMetrics 获取所有消费组监控指标
	GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error)
	// GetAllConsumerGroupsSummary 获取所有消费组摘要信息
	GetAllConsumerGroupsSummary(ctx context.Context) ([]ConsumerGroupMetrics, error)
}

// consumerQueryMySQL 消费组查询 MySQL 实现
type consumerQueryMySQL struct {
	db interfaces.DB
}

type lagLookupKey struct {
	Topic     string
	Partition uint
}

type partitionLagCounts struct {
	ConsumedCount int64 `gorm:"column:consumed_count"`
	LagCount      int64 `gorm:"column:lag_count"`
}

const partitionLagByGroupSQL = `
	SELECT
		p.topic,
		p.` + "`partition`" + `,
		COALESCE(SUM(CASE WHEN m.id >= p.subscription_start_watermark AND m.id < p.last_consumed_message_id THEN 1 ELSE 0 END), 0) AS consumed_count,
		COALESCE(SUM(CASE WHEN m.id > p.last_consumed_message_id THEN 1 ELSE 0 END), 0) AS lag_count
	FROM mq_consumer_group_consumption_progress p
	LEFT JOIN mq_messages m
		ON m.topic = p.topic AND m.` + "`partition`" + ` = p.` + "`partition`" + `
	WHERE p.group_id = ?
	GROUP BY p.topic, p.` + "`partition`" + `
`

const totalLagByGroupsSQL = `
	SELECT
		p.group_id,
		COALESCE(SUM(CASE WHEN m.id > p.last_consumed_message_id THEN 1 ELSE 0 END), 0) AS total_lag
	FROM mq_consumer_group_consumption_progress p
	LEFT JOIN mq_messages m
		ON m.topic = p.topic AND m.` + "`partition`" + ` = p.` + "`partition`" + `
	WHERE p.group_id IN ?
	GROUP BY p.group_id
`

// NewConsumerQuery 创建消费组查询实例
func NewConsumerQuery(db interfaces.DB) ConsumerQuery {
	return &consumerQueryMySQL{db: db}
}

func buildPartitionLagMetrics(topicName string, partition int, progress *consumerprogressrepo.ProgressPO, latestID, totalMsgCount int64, counts partitionLagCounts) PartitionLagMetrics {
	currentID := int64(-1)
	updateAt := int64(0)
	watermark := int64(0)
	if progress != nil {
		currentID = progress.LastConsumedMessageID
		updateAt = progress.UpdatedAt.UnixMilli()
		watermark = progress.SubscriptionStartWatermark
	}

	metrics := PartitionLagMetrics{
		Topic:                      topicName,
		Partition:                  partition,
		CurrentOffset:              currentID,
		LatestOffset:               latestID,
		Lag:                        counts.LagCount,
		SubscriptionStartWatermark: watermark,
		TotalMessageCount:          totalMsgCount,
		LastMessageId:              latestID,
		ConsumedMessages:           counts.ConsumedCount,
		RemainingMessages:          counts.LagCount,
		UpdatedAt:                  updateAt,
	}

	if counts.ConsumedCount+counts.LagCount > 0 {
		metrics.ConsumedPercentage = float64(counts.ConsumedCount) / float64(counts.ConsumedCount+counts.LagCount) * 100
	}

	return metrics
}

func (q *consumerQueryMySQL) batchLoadLagCounts(ctx context.Context, progressRecords []consumerprogressrepo.ProgressPO) (map[lagLookupKey]partitionLagCounts, error) {
	if len(progressRecords) == 0 {
		return map[lagLookupKey]partitionLagCounts{}, nil
	}

	type lagCountRow struct {
		Topic         string `gorm:"column:topic"`
		Partition     uint   `gorm:"column:partition"`
		ConsumedCount int64  `gorm:"column:consumed_count"`
		LagCount      int64  `gorm:"column:lag_count"`
	}

	groupedByID := make(map[string][]consumerprogressrepo.ProgressPO)
	for _, record := range progressRecords {
		groupedByID[record.GroupID] = append(groupedByID[record.GroupID], record)
	}

	result := make(map[lagLookupKey]partitionLagCounts, len(progressRecords))
	for groupID, records := range groupedByID {
		var rows []lagCountRow
		if err := q.db.WithContext(ctx).Raw(partitionLagByGroupSQL, groupID).Scan(&rows).Error; err != nil {
			return nil, err
		}

		rowMap := make(map[lagLookupKey]partitionLagCounts, len(rows))
		for _, row := range rows {
			rowMap[lagLookupKey{Topic: row.Topic, Partition: row.Partition}] = partitionLagCounts{
				ConsumedCount: row.ConsumedCount,
				LagCount:      row.LagCount,
			}
		}

		for _, record := range records {
			key := lagLookupKey{Topic: record.Topic, Partition: record.Partition}
			result[key] = rowMap[key]
		}
	}

	return result, nil
}

// GetSubscribedTopics 获取消费组订阅的 Topic 列表
func (q *consumerQueryMySQL) GetSubscribedTopics(ctx context.Context, groupID string) ([]string, error) {
	var records []consumerprogressrepo.ProgressPO
	if err := q.db.WithContext(ctx).Model(&consumerprogressrepo.ProgressPO{}).
		Where("group_id = ?", groupID).
		Find(&records).Error; err != nil {
		return nil, err
	}

	// 去重获取 Topic 列表
	topicMap := make(map[string]struct{})
	for _, r := range records {
		topicMap[r.Topic] = struct{}{}
	}
	topics := make([]string, 0, len(topicMap))
	for t := range topicMap {
		topics = append(topics, t)
	}
	return topics, nil
}

// GetProgressRecords 获取消费组的消费进度记录
func (q *consumerQueryMySQL) GetProgressRecords(ctx context.Context, groupID string) ([]consumerprogressrepo.ProgressPO, error) {
	var records []consumerprogressrepo.ProgressPO
	err := q.db.WithContext(ctx).Model(&consumerprogressrepo.ProgressPO{}).
		Where("group_id = ?", groupID).
		Find(&records).Error
	return records, err
}

// GetProgressRecord 获取消费组指定分区的消费进度
func (q *consumerQueryMySQL) GetProgressRecord(ctx context.Context, groupID string, topic string, partition uint) (*consumerprogressrepo.ProgressPO, error) {
	var record consumerprogressrepo.ProgressPO
	err := q.db.WithContext(ctx).
		Where("group_id = ? AND topic = ? AND `partition` = ?", groupID, topic, partition).
		First(&record).Error
	if err != nil {
		return nil, err
	}
	return &record, nil
}

// GetPartitionLagStats 获取分区延迟统计
// 使用单个聚合查询获取所有统计数据，减少数据库往返
func (q *consumerQueryMySQL) GetPartitionLagStats(ctx context.Context, topic string, partition uint, currentOffset int64, watermark int64) (*PartitionLag, error) {
	result := &PartitionLag{
		Topic:         topic,
		Partition:     partition,
		CurrentOffset: currentOffset,
	}

	// 使用单个聚合查询获取所有统计
	var stats struct {
		TotalCount    int64 `gorm:"column:total_count"`
		ConsumedCount int64 `gorm:"column:consumed_count"`
		LagCount      int64 `gorm:"column:lag_count"`
	}
	err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select(`
			COUNT(*) as total_count,
			SUM(CASE WHEN id >= ? AND id < ? THEN 1 ELSE 0 END) as consumed_count,
			SUM(CASE WHEN id > ? THEN 1 ELSE 0 END) as lag_count
		`, watermark, currentOffset, currentOffset).
		Where("topic = ? AND `partition` = ?", topic, partition).
		Scan(&stats).Error
	if err != nil {
		return nil, err
	}

	result.TotalMessageCount = stats.TotalCount
	result.ConsumedMessages = stats.ConsumedCount
	result.Lag = stats.LagCount
	result.RemainingMessages = stats.LagCount // lag 和 remaining 相同

	// 计算消费进度百分比
	if stats.ConsumedCount+stats.LagCount > 0 {
		result.ConsumedPercentage = float64(stats.ConsumedCount) / float64(stats.ConsumedCount+stats.LagCount) * 100
	}

	return result, nil
}

// GetConsumerGroupExtended 获取消费组扩展信息
func (q *consumerQueryMySQL) GetConsumerGroupExtended(ctx context.Context, groupID string) (*ConsumerGroupExtended, error) {
	result := &ConsumerGroupExtended{}

	// 获取代际信息
	var generationInfo struct {
		GenerationID int    `gorm:"column:generation_id"`
		LeaderID     string `gorm:"column:leader_id"`
		UpdatedAt    string `gorm:"column:updated_at"`
	}
	err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("generation_id, leader_id, updated_at").
		Where("group_id = ?", groupID).
		First(&generationInfo).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, err
	}
	result.GenerationID = generationInfo.GenerationID
	result.LeaderID = generationInfo.LeaderID

	// 获取成员信息
	var members []ConsumerMemberExtended
	err = q.db.WithContext(ctx).Table("mq_consumer_heartbeats").
		Select("consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at").
		Where("group_id = ?", groupID).
		Where("offline = ? AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 1 HOUR)", false).
		Order("offline ASC, last_heartbeat DESC, consumer_id ASC").
		Find(&members).Error
	if err != nil {
		return nil, err
	}
	result.Members = members

	// 获取消费进度
	var progress []struct {
		Topic                      string        `gorm:"column:topic"`
		Partition                  int           `gorm:"column:partition"`
		CommittedOffset            int64         `gorm:"column:committed_offset"`
		GenerationID               int           `gorm:"column:generation_id"`
		Metadata                   string        `gorm:"column:metadata"`
		UpdatedAt                  string        `gorm:"column:updated_at"`
		SubscriptionStartWatermark sql.NullInt64 `gorm:"column:subscription_start_watermark"`
	}
	err = q.db.WithContext(ctx).Table("mq_consumer_group_consumption_progress").
		Select("topic, `partition`, last_consumed_message_id as committed_offset, generation_id, metadata, updated_at, subscription_start_watermark").
		Where("group_id = ?", groupID).
		Find(&progress).Error
	if err != nil {
		return nil, err
	}

	result.SubscribedProgress = make([]SubscribedProgress, len(progress))
	for i, p := range progress {
		sp := SubscribedProgress{
			Topic:           p.Topic,
			Partition:       p.Partition,
			CommittedOffset: p.CommittedOffset,
			GenerationID:    p.GenerationID,
			Metadata:        p.Metadata,
		}
		if p.SubscriptionStartWatermark.Valid {
			w := p.SubscriptionStartWatermark.Int64
			sp.SubscriptionStartWatermark = &w
		}
		result.SubscribedProgress[i] = sp
	}

	return result, nil
}

// GetProgressRecordsDTO 获取消费进度记录 (返回 DTO)
func (q *consumerQueryMySQL) GetProgressRecordsDTO(ctx context.Context, groupID string) ([]ProgressRecord, error) {
	records, err := q.GetProgressRecords(ctx, groupID)
	if err != nil {
		return nil, err
	}

	result := make([]ProgressRecord, len(records))
	for i, r := range records {
		result[i] = ProgressRecord{
			Topic:                      r.Topic,
			Partition:                  r.Partition,
			CommittedOffset:            r.LastConsumedMessageID,
			GenerationID:               int(r.GenerationID),
			Metadata:                   r.Metadata,
			SubscriptionStartWatermark: r.SubscriptionStartWatermark,
			UpdatedAt:                  r.UpdatedAt,
		}
	}
	return result, nil
}

// GetConsumerGroupMetrics 获取消费组完整监控指标
// 从 metrics.go 迁移的核心业务逻辑
func (q *consumerQueryMySQL) GetConsumerGroupMetrics(ctx context.Context, groupID string) (*ConsumerGroupMetrics, error) {
	metrics := &ConsumerGroupMetrics{
		GroupID:        groupID,
		ProtocolType:   "consumer",
		AssignedTopics: []string{},
		Members:        []ConsumerMemberMetrics{},
		PartitionLags:  []PartitionLagMetrics{},
	}

	// 获取消费组代际信息
	var generation struct {
		GenerationID int `gorm:"column:generation_id"`
	}
	err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("generation_id").
		Where("group_id = ?", groupID).
		First(&generation).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return nil, fmt.Errorf("failed to get consumer group generation: %w", err)
	}
	metrics.GenerationID = int64(generation.GenerationID)

	// 获取活跃消费者
	var heartbeats []heartbeatrepo.HeartbeatPO
	heartbeatTimeout := 30 * time.Second
	cutoff := time.Now().Add(-heartbeatTimeout)
	err = q.db.WithContext(ctx).
		Where("group_id = ?", groupID).
		Where("offline = ? AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 1 HOUR)", false).
		Order("consumer_id ASC").
		Find(&heartbeats).Error
	if err != nil {
		return nil, err
	}

	var onlineCount int
	for _, item := range heartbeats {
		// 解析分配的分区 - 直接访问 AssignedPartitions 字段
		var assignment []PartitionInfo
		for _, p := range item.AssignedPartitions {
			assignment = append(assignment, PartitionInfo{Topic: p.Topic, Partition: p.Partition})
		}

		member := ConsumerMemberMetrics{
			ConsumerID:    item.ConsumerID,
			ClientID:      item.ConsumerID,
			Host:          "localhost",
			LastHeartbeat: item.LastHeartbeat,
			Assignment:    assignment,
		}
		metrics.Members = append(metrics.Members, member)

		if item.Offline || item.LastHeartbeat.Before(cutoff) {
			continue
		}
		onlineCount++
		metrics.LastHeartbeat = item.LastHeartbeat
	}

	if onlineCount == 0 {
		metrics.State = "Dead"
	} else {
		metrics.State = "Active"
	}

	// 获取消费进度记录（已有批量查询，一次取回该 group 的所有 progress）
	progressRecords, err := q.GetProgressRecords(ctx, groupID)
	if err != nil {
		return nil, err
	}

	metrics.AssignedTopics = lo.Uniq(lo.Map(progressRecords, func(item consumerprogressrepo.ProgressPO, _ int) string {
		return item.Topic
	}))

	if len(progressRecords) == 0 {
		return metrics, nil
	}

	lagCountMap, err := q.batchLoadLagCounts(ctx, progressRecords)
	if err != nil {
		return nil, err
	}

	progressMap := make(map[lagLookupKey]*consumerprogressrepo.ProgressPO, len(progressRecords))
	for i := range progressRecords {
		k := lagLookupKey{Topic: progressRecords[i].Topic, Partition: progressRecords[i].Partition}
		progressMap[k] = &progressRecords[i]
	}

	// 一次查询获取所有相关 topic+partition 的最新消息 ID 和总数
	type msgStatRow struct {
		Topic      string `gorm:"column:topic"`
		Partition  uint   `gorm:"column:partition"`
		LatestID   int64  `gorm:"column:latest_id"`
		TotalCount int64  `gorm:"column:total_count"`
	}

	topicNames := metrics.AssignedTopics
	var msgStats []msgStatRow
	err = q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
		Select("topic, `partition`, COALESCE(MAX(id), -1) AS latest_id, COUNT(*) AS total_count").
		Where("topic IN ?", topicNames).
		Group("topic, `partition`").
		Find(&msgStats).Error
	if err != nil {
		return nil, err
	}

	msgStatMap := make(map[lagLookupKey]*msgStatRow, len(msgStats))
	for i := range msgStats {
		k := lagLookupKey{Topic: msgStats[i].Topic, Partition: msgStats[i].Partition}
		msgStatMap[k] = &msgStats[i]
	}

	// 计算消费延迟
	var totalLag int64
	for _, topicName := range metrics.AssignedTopics {
		// 获取 Topic 信息
		var topicPO topicrepo.TopicPO
		if err := q.db.WithContext(ctx).Where("topic_name = ?", topicName).First(&topicPO).Error; err != nil {
			continue
		}

		for i := range topicPO.PartitionCount {
			k := lagLookupKey{Topic: topicName, Partition: i}

			progress := progressMap[k]
			ms := msgStatMap[k]
			var latestID int64 = -1
			var totalMsgCount int64
			if ms != nil {
				latestID = ms.LatestID
				totalMsgCount = ms.TotalCount
			}

			partitionLag := buildPartitionLagMetrics(topicName, int(i), progress, latestID, totalMsgCount, lagCountMap[k])
			metrics.PartitionLags = append(metrics.PartitionLags, partitionLag)
			totalLag += partitionLag.Lag
		}
	}
	metrics.Lag = totalLag

	return metrics, nil
}

// GetAllConsumerGroupsMetrics 获取所有消费组监控指标
// 批量查询优化：将多个 N+1 查询合并为少量批量查询
func (q *consumerQueryMySQL) GetAllConsumerGroupsMetrics(ctx context.Context) ([]ConsumerGroupMetrics, error) {
	// 1. 批量获取所有消费组代际信息
	type generationRow struct {
		GroupID      string `gorm:"column:group_id"`
		GenerationID int    `gorm:"column:generation_id"`
	}
	var allGenerations []generationRow
	if err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("group_id, generation_id").
		Find(&allGenerations).Error; err != nil {
		return nil, fmt.Errorf("failed to get all groups: %w", err)
	}
	if len(allGenerations) == 0 {
		return nil, nil
	}

	generationMap := make(map[string]int, len(allGenerations))
	groupIDs := make([]string, 0, len(allGenerations))
	for _, g := range allGenerations {
		generationMap[g.GroupID] = g.GenerationID
		groupIDs = append(groupIDs, g.GroupID)
	}

	// 2. 批量获取所有活跃消费者心跳
	var allHeartbeats []heartbeatrepo.HeartbeatPO
	if err := q.db.WithContext(ctx).
		Where("group_id IN ?", groupIDs).
		Where("offline = ? AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 1 HOUR)", false).
		Order("group_id ASC, consumer_id ASC").
		Find(&allHeartbeats).Error; err != nil {
		return nil, err
	}
	heartbeatsByGroup := make(map[string][]heartbeatrepo.HeartbeatPO)
	for _, hb := range allHeartbeats {
		heartbeatsByGroup[hb.GroupID] = append(heartbeatsByGroup[hb.GroupID], hb)
	}

	// 3. 批量获取所有消费进度
	var allProgress []consumerprogressrepo.ProgressPO
	if err := q.db.WithContext(ctx).Model(&consumerprogressrepo.ProgressPO{}).
		Where("group_id IN ?", groupIDs).
		Find(&allProgress).Error; err != nil {
		return nil, err
	}
	progressByGroup := make(map[string][]consumerprogressrepo.ProgressPO)
	allTopicNames := make(map[string]struct{})
	for _, p := range allProgress {
		progressByGroup[p.GroupID] = append(progressByGroup[p.GroupID], p)
		allTopicNames[p.Topic] = struct{}{}
	}

	// 4. 批量获取所有相关 Topic 信息
	topicNameList := make([]string, 0, len(allTopicNames))
	for t := range allTopicNames {
		topicNameList = append(topicNameList, t)
	}
	topicMap := make(map[string]*topicrepo.TopicPO)
	if len(topicNameList) > 0 {
		var topics []topicrepo.TopicPO
		if err := q.db.WithContext(ctx).Where("topic_name IN ?", topicNameList).Find(&topics).Error; err != nil {
			return nil, err
		}
		for i := range topics {
			topicMap[topics[i].TopicName] = &topics[i]
		}
	}

	// 5. 批量获取所有相关 topic+partition 的消息统计
	type msgStatRow struct {
		Topic      string `gorm:"column:topic"`
		Partition  uint   `gorm:"column:partition"`
		LatestID   int64  `gorm:"column:latest_id"`
		TotalCount int64  `gorm:"column:total_count"`
	}
	msgStatMap := make(map[lagLookupKey]*msgStatRow)
	if len(topicNameList) > 0 {
		var msgStats []msgStatRow
		if err := q.db.WithContext(ctx).Model(&messagerepo.MessagePO{}).
			Select("topic, `partition`, COALESCE(MAX(id), -1) AS latest_id, COUNT(*) AS total_count").
			Where("topic IN ?", topicNameList).
			Group("topic, `partition`").
			Find(&msgStats).Error; err != nil {
			return nil, err
		}
		for i := range msgStats {
			k := lagLookupKey{Topic: msgStats[i].Topic, Partition: msgStats[i].Partition}
			msgStatMap[k] = &msgStats[i]
		}
	}

	lagCountMap, err := q.batchLoadLagCounts(ctx, allProgress)
	if err != nil {
		return nil, err
	}

	// 6. 在内存中组装每个消费组的指标
	heartbeatTimeout := 30 * time.Second
	cutoff := time.Now().Add(-heartbeatTimeout)

	metricsSlice := make([]ConsumerGroupMetrics, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		metrics := ConsumerGroupMetrics{
			GroupID:        groupID,
			ProtocolType:   "consumer",
			AssignedTopics: []string{},
			Members:        []ConsumerMemberMetrics{},
			PartitionLags:  []PartitionLagMetrics{},
			GenerationID:   int64(generationMap[groupID]),
		}

		// 处理心跳/成员
		var onlineCount int
		for _, item := range heartbeatsByGroup[groupID] {
			var assignment []PartitionInfo
			for _, p := range item.AssignedPartitions {
				assignment = append(assignment, PartitionInfo{Topic: p.Topic, Partition: p.Partition})
			}
			metrics.Members = append(metrics.Members, ConsumerMemberMetrics{
				ConsumerID:    item.ConsumerID,
				ClientID:      item.ConsumerID,
				Host:          "localhost",
				LastHeartbeat: item.LastHeartbeat,
				Assignment:    assignment,
			})
			if !item.Offline && !item.LastHeartbeat.Before(cutoff) {
				onlineCount++
				metrics.LastHeartbeat = item.LastHeartbeat
			}
		}
		if onlineCount == 0 {
			metrics.State = "Dead"
		} else {
			metrics.State = "Active"
		}

		// 处理消费进度
		progressRecords := progressByGroup[groupID]
		metrics.AssignedTopics = lo.Uniq(lo.Map(progressRecords, func(item consumerprogressrepo.ProgressPO, _ int) string {
			return item.Topic
		}))

		if len(progressRecords) == 0 {
			metricsSlice = append(metricsSlice, metrics)
			continue
		}

		// 建立 progress 索引
		progressMap := make(map[lagLookupKey]*consumerprogressrepo.ProgressPO, len(progressRecords))
		for i := range progressRecords {
			k := lagLookupKey{Topic: progressRecords[i].Topic, Partition: progressRecords[i].Partition}
			progressMap[k] = &progressRecords[i]
		}

		// 计算消费延迟
		var totalLag int64
		for _, topicName := range metrics.AssignedTopics {
			tp := topicMap[topicName]
			if tp == nil {
				continue
			}
			for i := range tp.PartitionCount {
				k := lagLookupKey{Topic: topicName, Partition: i}

				progress := progressMap[k]
				ms := msgStatMap[k]
				var latestID int64 = -1
				var totalMsgCount int64
				if ms != nil {
					latestID = ms.LatestID
					totalMsgCount = ms.TotalCount
				}

				partitionLag := buildPartitionLagMetrics(topicName, int(i), progress, latestID, totalMsgCount, lagCountMap[k])
				metrics.PartitionLags = append(metrics.PartitionLags, partitionLag)
				totalLag += partitionLag.Lag
			}
		}
		metrics.Lag = totalLag
		metricsSlice = append(metricsSlice, metrics)
	}

	return metricsSlice, nil
}

func (q *consumerQueryMySQL) GetAllConsumerGroupsSummary(ctx context.Context) ([]ConsumerGroupMetrics, error) {
	type generationRow struct {
		GroupID      string `gorm:"column:group_id"`
		GenerationID int    `gorm:"column:generation_id"`
	}
	var allGenerations []generationRow
	if err := q.db.WithContext(ctx).Table("mq_consumer_group_generations").
		Select("group_id, generation_id").
		Find(&allGenerations).Error; err != nil {
		return nil, fmt.Errorf("failed to get all groups: %w", err)
	}
	if len(allGenerations) == 0 {
		return nil, nil
	}

	generationMap := make(map[string]int, len(allGenerations))
	groupIDs := make([]string, 0, len(allGenerations))
	for _, item := range allGenerations {
		generationMap[item.GroupID] = item.GenerationID
		groupIDs = append(groupIDs, item.GroupID)
	}

	var allHeartbeats []heartbeatrepo.HeartbeatPO
	if err := q.db.WithContext(ctx).
		Where("group_id IN ?", groupIDs).
		Where("offline = ? AND last_heartbeat > DATE_SUB(NOW(), INTERVAL 1 HOUR)", false).
		Order("group_id ASC, consumer_id ASC").
		Find(&allHeartbeats).Error; err != nil {
		return nil, err
	}

	heartbeatsByGroup := make(map[string][]heartbeatrepo.HeartbeatPO)
	for _, hb := range allHeartbeats {
		heartbeatsByGroup[hb.GroupID] = append(heartbeatsByGroup[hb.GroupID], hb)
	}

	type lagRow struct {
		GroupID  string `gorm:"column:group_id"`
		TotalLag int64  `gorm:"column:total_lag"`
	}
	var lagRows []lagRow
	if err := q.db.WithContext(ctx).Raw(totalLagByGroupsSQL, groupIDs).Scan(&lagRows).Error; err != nil {
		return nil, err
	}
	lagByGroup := make(map[string]int64, len(lagRows))
	for _, row := range lagRows {
		lagByGroup[row.GroupID] = row.TotalLag
	}

	heartbeatTimeout := 30 * time.Second
	cutoff := time.Now().Add(-heartbeatTimeout)
	result := make([]ConsumerGroupMetrics, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		heartbeats := heartbeatsByGroup[groupID]
		metrics := ConsumerGroupMetrics{
			GroupID:        groupID,
			ProtocolType:   "consumer",
			AssignedTopics: []string{},
			Members:        make([]ConsumerMemberMetrics, 0, len(heartbeats)),
			PartitionLags:  []PartitionLagMetrics{},
			GenerationID:   int64(generationMap[groupID]),
			Lag:            lagByGroup[groupID],
		}

		var onlineCount int
		for _, item := range heartbeats {
			var assignment []PartitionInfo
			for _, p := range item.AssignedPartitions {
				assignment = append(assignment, PartitionInfo{Topic: p.Topic, Partition: p.Partition})
			}
			metrics.Members = append(metrics.Members, ConsumerMemberMetrics{
				ConsumerID:    item.ConsumerID,
				ClientID:      item.ConsumerID,
				Host:          "localhost",
				LastHeartbeat: item.LastHeartbeat,
				Assignment:    assignment,
			})
			if !item.Offline && !item.LastHeartbeat.Before(cutoff) {
				onlineCount++
				metrics.LastHeartbeat = item.LastHeartbeat
			}
		}

		if onlineCount == 0 {
			metrics.State = "Dead"
		} else {
			metrics.State = "Active"
		}

		result = append(result, metrics)
	}

	return result, nil
}
