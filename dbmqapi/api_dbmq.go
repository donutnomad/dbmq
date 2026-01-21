package dbmqapi

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/db"

	"gorm.io/gorm"
)

// DBMQAPI DBMQ 专用 API
// @TAG(DBMQ)
// @PREFIX(/api/v1/dbmq)
type DBMQAPI interface {
	// GetStats 获取统计信息
	// @GET(/stats)
	GetStats(ctx context.Context) (DBMQStatsResp, error)
	// GetTopicMessages 获取 Topic 消息列表
	// @GET(/topics/{topicName}/messages)
	GetTopicMessages(ctx context.Context, topicName string, req GetTopicMessagesReq) (TopicMessagesResp, error)
	// GetPartitionStats 获取分区统计信息
	// @GET(/topics/{topicName}/partitions/{partitionId}/stats)
	GetPartitionStats(ctx context.Context, topicName string, partitionId uint) (PartitionStats, error)
	// GetConsumerGroupExtended 获取消费组扩展信息
	// @GET(/consumer-groups/{groupId}/extended)
	GetConsumerGroupExtended(ctx context.Context, groupId string) (ConsumerGroupExtendedResp, error)
}

type dbmqAPI struct {
	deps *Deps
}

func NewDBMQAPI(deps *Deps) DBMQAPI {
	return &dbmqAPI{deps: deps}
}

func (a *dbmqAPI) GetStats(ctx context.Context) (DBMQStatsResp, error) {
	clusterMetrics, err := a.deps.MetricsClient.GetClusterMetrics(ctx)
	if err != nil {
		return DBMQStatsResp{}, err
	}

	brokerMetrics, err := a.deps.MetricsClient.GetBrokerMetrics(ctx)
	if err != nil {
		return DBMQStatsResp{}, err
	}

	return DBMQStatsResp{
		Cluster: ClusterMetricsResp{
			TopicCount:         clusterMetrics.TopicCount,
			PartitionCount:     clusterMetrics.PartitionCount,
			ConsumerGroupCount: clusterMetrics.ConsumerGroups,
			TotalMessages:      clusterMetrics.MessageCount,
		},
		Broker: BrokerResp{
			BrokerID: brokerMetrics.BrokerID,
			Host:     brokerMetrics.Host,
			Port:     brokerMetrics.Port,
			Version:  brokerMetrics.Version,
			Uptime:   fmt.Sprintf("%ds", brokerMetrics.Uptime),
		},
		System: SystemInfoResp{
			Version: brokerMetrics.Version,
		},
	}, nil
}

func (a *dbmqAPI) GetTopicMessages(ctx context.Context, topicName string, req GetTopicMessagesReq) (TopicMessagesResp, error) {
	limit := req.Limit
	if limit <= 0 || limit > 500 {
		limit = 50
	}

	var messages []db.Message
	query := a.deps.DB.WithContext(ctx).Where("topic = ?", topicName)

	if req.Partition != nil {
		query = query.Where("`partition` = ?", *req.Partition)
	}

	if req.FromTime != "" {
		if t, err := time.Parse(time.RFC3339, req.FromTime); err == nil {
			query = query.Where("created_at >= ?", t)
		}
	}
	if req.ToTime != "" {
		if t, err := time.Parse(time.RFC3339, req.ToTime); err == nil {
			query = query.Where("created_at <= ?", t)
		}
	}

	if req.Search != "" {
		query = query.Where("(message_key LIKE ? OR body LIKE ?)", "%"+req.Search+"%", "%"+req.Search+"%")
	}

	var total int64
	query.Model(&db.Message{}).Count(&total)

	err := query.Order("created_at DESC").Offset(int(req.Offset)).Limit(limit).Find(&messages).Error
	if err != nil {
		return TopicMessagesResp{}, err
	}

	messageDTOs := make([]MessageDTO, len(messages))
	for i, msg := range messages {
		messageDTOs[i] = MessageDTO{
			ID:        msg.ID,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.ID,
			Key:       msg.MessageKey,
			Value:     string(msg.Body),
			Timestamp: msg.CreatedAt.Format(time.RFC3339),
			Size:      len(msg.Body),
			Headers:   msg.Headers.Data(),
		}
	}

	partitionStr := ""
	if req.Partition != nil {
		partitionStr = fmt.Sprintf("%d", *req.Partition)
	}

	return TopicMessagesResp{
		Topic:     topicName,
		Partition: partitionStr,
		Offset:    req.Offset,
		Limit:     limit,
		Search:    req.Search,
		Messages:  messageDTOs,
		Total:     total,
	}, nil
}

func (a *dbmqAPI) GetPartitionStats(ctx context.Context, topicName string, partitionId uint) (PartitionStats, error) {
	stats, err := getPartitionStatsFromDB(ctx, a.deps.DB, topicName, partitionId)
	if err != nil {
		return PartitionStats{}, err
	}
	return *stats, nil
}

func (a *dbmqAPI) GetConsumerGroupExtended(ctx context.Context, groupId string) (ConsumerGroupExtendedResp, error) {
	group, err := a.deps.MetricsClient.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		return ConsumerGroupExtendedResp{}, err
	}

	var generationInfo struct {
		GenerationID int       `json:"generationId"`
		LeaderID     string    `json:"leaderId"`
		UpdatedAt    time.Time `json:"updatedAt"`
	}
	err = a.deps.DB.Table("mq_consumer_group_generations").
		Select("generation_id, leader_id, updated_at").
		Where("group_id = ?", groupId).
		First(&generationInfo).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return ConsumerGroupExtendedResp{}, err
	}

	var members []struct {
		ConsumerID         string     `json:"consumerId"`
		GenerationID       int        `json:"generationId"`
		SubscribedTopics   string     `json:"subscribedTopics"`
		AssignedPartitions string     `json:"assignedPartitions"`
		Offline            bool       `json:"offline"`
		LastHeartbeat      time.Time  `json:"lastHeartbeat"`
		OfflineAt          *time.Time `json:"offlineAt"`
	}
	err = a.deps.DB.Table("mq_consumer_heartbeats").
		Select("consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at").
		Where("group_id = ?", groupId).
		Order("offline ASC, last_heartbeat DESC").
		Find(&members).Error
	if err != nil {
		return ConsumerGroupExtendedResp{}, err
	}

	var offsets []struct {
		Topic                 string        `json:"topic"`
		Partition             int           `json:"partition"`
		CommittedOffset       int64         `json:"committedOffset"`
		GenerationID          int           `json:"generationId"`
		Metadata              string        `json:"metadata"`
		UpdatedAt             time.Time     `json:"updatedAt"`
		InitialTopicWatermark sql.NullInt64 `json:"initialTopicWatermark"`
	}
	err = a.deps.DB.Table("mq_consumer_group_consumption_progress").
		Select("topic, `partition`, last_consumed_message_id as committed_offset, generation_id, metadata, updated_at, subscription_start_watermark as initial_topic_watermark").
		Where("group_id = ?", groupId).
		Find(&offsets).Error
	if err != nil {
		return ConsumerGroupExtendedResp{}, err
	}

	heartbeatTimeout := 30 * time.Second
	enhancedMembers := make([]ConsumerMemberDTO, 0, len(members))

	for _, detail := range members {
		memberDTO := ConsumerMemberDTO{
			MemberID:      detail.ConsumerID,
			ClientID:      detail.ConsumerID,
			Host:          "unknown",
			GenerationID:  detail.GenerationID,
			Offline:       detail.Offline,
			LastHeartbeat: detail.LastHeartbeat.Format(time.RFC3339),
			Assignment:    make(map[string][]int),
		}

		if detail.OfflineAt != nil {
			offlineAt := detail.OfflineAt.Format(time.RFC3339)
			memberDTO.OfflineAt = &offlineAt
		}

		if detail.SubscribedTopics != "" {
			var subscribedTopics []string
			if err := json.Unmarshal([]byte(detail.SubscribedTopics), &subscribedTopics); err == nil {
				memberDTO.SubscribedTopics = subscribedTopics
			}
		}

		if detail.AssignedPartitions != "" {
			var partitionInfoArray []map[string]any
			if err := json.Unmarshal([]byte(detail.AssignedPartitions), &partitionInfoArray); err == nil {
				for _, partitionInfo := range partitionInfoArray {
					if topic, ok := partitionInfo["Topic"].(string); ok {
						if partition, ok := partitionInfo["Partition"].(float64); ok {
							memberDTO.Assignment[topic] = append(memberDTO.Assignment[topic], int(partition))
						}
					}
				}
			}
		}

		isTimeout := time.Since(detail.LastHeartbeat) > heartbeatTimeout
		if detail.Offline {
			memberDTO.Status = "offline"
		} else if isTimeout {
			memberDTO.Status = "timeout"
		} else {
			memberDTO.Status = "online"
		}

		enhancedMembers = append(enhancedMembers, memberDTO)
	}

	enhancedLags := make([]PartitionLagExt, 0, len(group.PartitionLags))
	for _, lag := range group.PartitionLags {
		lagExt := PartitionLagExt{
			Topic:         lag.Topic,
			Partition:     lag.Partition,
			CurrentOffset: lag.CurrentOffset,
			LatestOffset:  lag.LatestOffset,
			Lag:           lag.Lag,
		}

		partitionStats, err := getPartitionStatsFromDB(ctx, a.deps.DB, lag.Topic, uint(lag.Partition))
		if err == nil && partitionStats != nil {
			lagExt.FirstMessageID = partitionStats.FirstMessageID
			lagExt.LastMessageID = partitionStats.LastMessageID
			lagExt.TotalMessageCount = partitionStats.MessageCount
			lagExt.PartitionSizeBytes = partitionStats.SizeBytes

			if partitionStats.MessageCount > 0 && lag.CurrentOffset >= partitionStats.FirstMessageID {
				consumedMessages := lag.CurrentOffset - partitionStats.FirstMessageID + 1
				consumedMessages = min(consumedMessages, partitionStats.MessageCount)
				lagExt.ConsumedMessages = consumedMessages
				lagExt.RemainingMessages = partitionStats.MessageCount - consumedMessages
				lagExt.ConsumedPercentage = float64(consumedMessages) / float64(partitionStats.MessageCount) * 100
			}
		}

		for _, offset := range offsets {
			if offset.Topic == lag.Topic && offset.Partition == lag.Partition {
				lagExt.Metadata = offset.Metadata
				lagExt.UpdatedAt = offset.UpdatedAt.Format(time.RFC3339)
				lagExt.GenerationID = offset.GenerationID
				if offset.InitialTopicWatermark.Valid {
					lagExt.InitialTopicWatermark = &offset.InitialTopicWatermark.Int64
				}
				break
			}
		}

		enhancedLags = append(enhancedLags, lagExt)
	}

	return ConsumerGroupExtendedResp{
		Members:            enhancedMembers,
		PartitionLags:      enhancedLags,
		GenerationID:       generationInfo.GenerationID,
		LastActivity:       generationInfo.UpdatedAt.Format(time.RFC3339),
		Coordinator:        "coordinator",
		CommitMode:         "manual",
		AssignmentStrategy: "range",
	}, nil
}
