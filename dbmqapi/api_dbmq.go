package dbmqapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/query"
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
	// ResendMessages 重发消息
	// @POST(/messages/resend)
	ResendMessages(ctx context.Context, req ResendMessagesReq) (ResendMessagesResp, error)
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

	// 使用查询层搜索消息
	searchReq := query.MessageSearchRequest{
		Topic:    topicName,
		Offset:   req.Offset,
		Limit:    limit,
		Search:   req.Search,
		FromTime: query.ParseTime(req.FromTime),
		ToTime:   query.ParseTime(req.ToTime),
	}
	if req.Partition != nil {
		searchReq.Partition = req.Partition
	}

	result, err := a.deps.MessageQuery.Search(ctx, searchReq)
	if err != nil {
		return TopicMessagesResp{}, err
	}

	// 转换为 API 响应格式
	messageDTOs := make([]MessageDTO, len(result.Messages))
	for i, msg := range result.Messages {
		messageDTOs[i] = MessageDTO{
			ID:        msg.ID,
			Topic:     msg.Topic,
			Partition: msg.Partition,
			Offset:    msg.ID,
			Key:       msg.MessageKey,
			Value:     string(msg.Body),
			Timestamp: msg.CreatedAt.Format(time.RFC3339),
			Size:      len(msg.Body),
			Headers:   msg.Headers,
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
		Total:     result.Total,
	}, nil
}

func (a *dbmqAPI) GetPartitionStats(ctx context.Context, topicName string, partitionId uint) (PartitionStats, error) {
	// 使用查询层获取分区统计信息
	stats, err := a.deps.TopicQuery.GetPartitionStats(ctx, topicName, partitionId)
	if err != nil {
		return PartitionStats{}, err
	}
	return PartitionStats{
		Partition:      stats.Partition,
		FirstMessageID: stats.FirstMessageID,
		LastMessageID:  stats.LastMessageID,
		MessageCount:   stats.MessageCount,
		SizeBytes:      stats.SizeBytes,
		CreatedAt:      stats.CreatedAt,
		UpdatedAt:      stats.UpdatedAt,
	}, nil
}

func (a *dbmqAPI) GetConsumerGroupExtended(ctx context.Context, groupId string) (ConsumerGroupExtendedResp, error) {
	group, err := a.deps.MetricsClient.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		return ConsumerGroupExtendedResp{}, err
	}

	// 使用查询层获取消费组扩展信息
	extended, err := a.deps.ConsumerQuery.GetConsumerGroupExtended(ctx, groupId)
	if err != nil {
		return ConsumerGroupExtendedResp{}, err
	}

	heartbeatTimeout := 30 * time.Second
	enhancedMembers := make([]ConsumerMemberDTO, 0, len(extended.Members))

	for _, detail := range extended.Members {
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

		// 使用查询层获取分区统计信息
		partitionStats, err := a.deps.TopicQuery.GetPartitionStats(ctx, lag.Topic, uint(lag.Partition))
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

		// 从查询层获取的 progress 中查找对应的元数据
		for _, progress := range extended.SubscribedProgress {
			if progress.Topic == lag.Topic && progress.Partition == lag.Partition {
				lagExt.Metadata = progress.Metadata
				lagExt.UpdatedAt = progress.UpdatedAt.Format(time.RFC3339)
				lagExt.GenerationID = progress.GenerationID
				if progress.SubscriptionStartWatermark != nil {
					lagExt.InitialTopicWatermark = progress.SubscriptionStartWatermark
				}
				break
			}
		}

		enhancedLags = append(enhancedLags, lagExt)
	}

	return ConsumerGroupExtendedResp{
		Members:            enhancedMembers,
		PartitionLags:      enhancedLags,
		GenerationID:       extended.GenerationID,
		LastActivity:       extended.UpdatedAt.Format(time.RFC3339),
		Coordinator:        "coordinator",
		CommitMode:         "manual",
		AssignmentStrategy: "range",
	}, nil
}

func (a *dbmqAPI) ResendMessages(ctx context.Context, req ResendMessagesReq) (ResendMessagesResp, error) {
	if len(req.Messages) == 0 {
		return ResendMessagesResp{}, fmt.Errorf("messages cannot be empty")
	}

	if a.deps.Producer == nil {
		return ResendMessagesResp{}, fmt.Errorf("producer not configured")
	}

	results := make([]ResendResult, 0, len(req.Messages))
	successCount := 0
	failedCount := 0

	// 逐个获取并重发消息
	for _, msgItem := range req.Messages {
		result := ResendResult{
			OriginalMessageID: msgItem.MessageID,
			Success:           false,
		}

		// 通过查询层查找消息
		searchResult, err := a.deps.MessageQuery.Search(ctx, query.MessageSearchRequest{
			Topic:  msgItem.Topic,
			Offset: msgItem.MessageID,
			Limit:  100, // 获取一批消息以找到匹配的 ID
		})

		if err != nil {
			errMsg := fmt.Sprintf("failed to query message: %v", err)
			result.Error = &errMsg
			results = append(results, result)
			failedCount++
			continue
		}

		// 找到 ID 匹配的消息
		var originalMsg *query.MessageRecord
		for i := range searchResult.Messages {
			if searchResult.Messages[i].ID == msgItem.MessageID {
				originalMsg = &searchResult.Messages[i]
				break
			}
		}

		if originalMsg == nil {
			errMsg := "message not found"
			result.Error = &errMsg
			results = append(results, result)
			failedCount++
			continue
		}

		// 构建重发消息
		targetTopic := originalMsg.Topic
		if req.TargetTopic != nil && *req.TargetTopic != "" {
			targetTopic = *req.TargetTopic
		}

		key := originalMsg.MessageKey
		if msgItem.Key != nil {
			key = *msgItem.Key
		}

		// 合并 Headers
		headers := make(map[string]string)
		for k, v := range originalMsg.Headers {
			headers[k] = v
		}
		for k, v := range req.OverrideHeaders {
			headers[k] = v
		}

		// 发送消息
		sendResult, err := a.deps.Producer.Send(ctx, dbmq.ProducerMessage{
			Topic:   targetTopic,
			Key:     key,
			Value:   originalMsg.Body,
			Headers: headers,
		})

		if err != nil {
			errMsg := err.Error()
			result.Error = &errMsg
			results = append(results, result)
			failedCount++
			continue
		}

		// 成功
		result.Success = true
		newOffset := sendResult.Offset
		result.NewMessageID = &newOffset
		result.NewOffset = &newOffset
		results = append(results, result)
		successCount++
	}

	return ResendMessagesResp{
		SuccessCount: successCount,
		FailedCount:  failedCount,
		Results:      results,
	}, nil
}
