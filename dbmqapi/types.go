package dbmqapi

import "time"

// ==================== Common Types ====================

// EmptyResp 空响应
type EmptyResp struct{}

// MessageResp 消息响应
type MessageResp struct {
	Message string `json:"message"`
}

// ==================== Health API Types ====================

// HealthResp 健康检查响应
type HealthResp struct {
	Status     string                  `json:"status"`
	Timestamp  string                  `json:"timestamp"`
	Components map[string]HealthStatus `json:"components"`
}

// HealthStatus 组件健康状态
type HealthStatus struct {
	Status string `json:"status"`
}

// InfoResp 信息响应
type InfoResp struct {
	App   AppInfo   `json:"app"`
	Build BuildInfo `json:"build"`
}

// AppInfo 应用信息
type AppInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Version     string `json:"version"`
}

// BuildInfo 构建信息
type BuildInfo struct {
	Time    string `json:"time"`
	Version string `json:"version"`
}

// ==================== Cluster API Types ====================

// ClusterResp 集群信息响应
type ClusterResp struct {
	ClusterID   string `json:"clusterId"`
	Name        string `json:"name"`
	BrokerCount int    `json:"brokerCount"`
	Status      string `json:"status"`
}

// ClusterMetricsResp 集群指标响应
type ClusterMetricsResp struct {
	TopicCount         int   `json:"topicCount"`
	PartitionCount     int   `json:"partitionCount"`
	ConsumerGroupCount int   `json:"consumerGroupCount"`
	TotalMessages      int64 `json:"totalMessages"`
	TotalSizeBytes     int64 `json:"totalSizeBytes"`
}

// BrokerResp Broker 信息响应
type BrokerResp struct {
	BrokerID int    `json:"brokerId"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Version  string `json:"version"`
	Uptime   string `json:"uptime"`
}

// ==================== Topic API Types ====================

// GetTopicsReq 获取 Topic 列表请求
type GetTopicsReq struct {
	IncludePartitionStats bool `form:"includePartitionStats"` // 是否包含分区统计信息
}

// TopicResp Topic 信息响应
type TopicResp struct {
	Name           string           `json:"name"`
	PartitionCount uint             `json:"partitionCount"`
	MessageCount   int64            `json:"messageCount"`
	SizeBytes      int64            `json:"sizeBytes"`
	CreatedAt      string           `json:"createdAt"`
	PartitionStats []PartitionStats `json:"partitionStats,omitempty"`
}

// PartitionStats 分区统计信息
type PartitionStats struct {
	Partition      uint   `json:"partition"`
	FirstMessageID int64  `json:"firstMessageId"`
	LastMessageID  int64  `json:"lastMessageId"`
	MessageCount   int64  `json:"messageCount"`
	SizeBytes      int64  `json:"sizeBytes"`
	CreatedAt      string `json:"createdAt,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
}

// CreateTopicReq 创建 Topic 请求
type CreateTopicReq struct {
	Name          string          `json:"name" binding:"required,min=1,max=255"`  // Topic 名称
	NumPartitions uint            `json:"numPartitions" binding:"required,min=1"` // 分区数量
	Config        *TopicConfigReq `json:"config,omitempty"`                       // Topic 配置
}

// TopicConfigReq Topic 配置
type TopicConfigReq struct {
	RetentionMs *int64 `json:"retention_ms,omitempty"` // 消息保留时间（毫秒）
}

// ==================== Topic Messages API Types ====================

// GetTopicMessagesReq 获取 Topic 消息列表请求
type GetTopicMessagesReq struct {
	Partition *uint  `form:"partition"`               // 分区号（可选）
	Offset    int64  `form:"offset"`                  // 偏移量
	Limit     int    `form:"limit" binding:"max=500"` // 限制条数（最大 500）
	Search    string `form:"search"`                  // 搜索关键字
	FromTime  string `form:"from_time"`               // 开始时间 RFC3339
	ToTime    string `form:"to_time"`                 // 结束时间 RFC3339
}

// TopicMessagesResp Topic 消息列表响应
type TopicMessagesResp struct {
	Topic     string       `json:"topic"`
	Partition string       `json:"partition"`
	Offset    int64        `json:"offset"`
	Limit     int          `json:"limit"`
	Search    string       `json:"search"`
	Messages  []MessageDTO `json:"messages"`
	Total     int64        `json:"total"`
}

// MessageDTO 消息数据传输对象
type MessageDTO struct {
	ID        int64             `json:"id"`
	Topic     string            `json:"topic"`
	Partition uint              `json:"partition"`
	Offset    int64             `json:"offset"`
	Key       string            `json:"key"`
	Value     string            `json:"value"`
	Timestamp string            `json:"timestamp"`
	Size      int               `json:"size"`
	Headers   map[string]string `json:"headers"`
}

// ResendMessagesReq 重发消息请求
type ResendMessagesReq struct {
	// Messages 要重发的消息列表（包含 Topic 和 ID）
	Messages []ResendMessageItem `json:"messages"`
	// TargetTopic 目标 Topic（可选，如果指定则所有消息发送到此 Topic）
	TargetTopic *string `json:"targetTopic,omitempty"`
	// OverrideHeaders 覆盖的 Headers（可选）
	OverrideHeaders map[string]string `json:"overrideHeaders,omitempty"`
}

// ResendMessageItem 单个重发消息项
type ResendMessageItem struct {
	// Topic 消息所在的 Topic
	Topic string `json:"topic"`
	// MessageID 消息 ID
	MessageID int64 `json:"messageId"`
	// Key 新的消息键（可选，默认使用原消息的 Key）
	Key *string `json:"key,omitempty"`
}

// ResendMessagesResp 重发消息响应
type ResendMessagesResp struct {
	// SuccessCount 成功重发的消息数量
	SuccessCount int `json:"successCount"`
	// FailedCount 失败的消息数量
	FailedCount int `json:"failedCount"`
	// Results 每条消息的重发结果
	Results []ResendResult `json:"results"`
}

// ResendResult 单个消息的重发结果
type ResendResult struct {
	// OriginalMessageID 原始消息 ID
	OriginalMessageID int64 `json:"originalMessageId"`
	// Success 是否成功
	Success bool `json:"success"`
	// NewMessageID 新消息的 ID（成功时）
	NewMessageID *int64 `json:"newMessageId,omitempty"`
	// NewOffset 新消息的 Offset（成功时）
	NewOffset *int64 `json:"newOffset,omitempty"`
	// Error 错误信息（失败时）
	Error *string `json:"error,omitempty"`
}

// ==================== Consumer Group API Types ====================

// ConsumerGroupResp 消费组响应
type ConsumerGroupResp struct {
	GroupID       string              `json:"groupId"`
	State         string              `json:"state"`
	MemberCount   int                 `json:"memberCount"`
	TotalLag      int64               `json:"totalLag"`
	PartitionLags []PartitionLagResp  `json:"partitionLags"`
	Members       []ConsumerMemberDTO `json:"members,omitempty"`
}

// PartitionLagResp 分区延迟响应
type PartitionLagResp struct {
	Topic              string  `json:"topic"`
	Partition          int     `json:"partition"`
	CurrentOffset      int64   `json:"currentOffset"`
	LatestOffset       int64   `json:"latestOffset"`
	Lag                int64   `json:"lag"`
	ConsumedMessages   int64   `json:"consumedMessages"`   // 已消费的消息数
	RemainingMessages  int64   `json:"remainingMessages"`  // 剩余未消费的消息数
	ConsumedPercentage float64 `json:"consumedPercentage"` // 消费进度百分比
}

// ConsumerMemberDTO 消费者成员数据传输对象
type ConsumerMemberDTO struct {
	MemberID         string           `json:"memberId"`
	ClientID         string           `json:"clientId"`
	Host             string           `json:"host"`
	GenerationID     int              `json:"generationId"`
	Offline          bool             `json:"offline"`
	LastHeartbeat    string           `json:"lastHeartbeat"`
	OfflineAt        *string          `json:"offlineAt,omitempty"`
	SubscribedTopics []string         `json:"subscribedTopics,omitempty"`
	Assignment       map[string][]int `json:"assignment"`
	Status           string           `json:"status"` // online/offline/timeout
}

// ConsumerGroupExtendedResp 消费组扩展响应
type ConsumerGroupExtendedResp struct {
	Members            []ConsumerMemberDTO `json:"members"`
	PartitionLags      []PartitionLagExt   `json:"partitionLags"`
	GenerationID       int                 `json:"generationId"`
	LastActivity       string              `json:"lastActivity"`
	Coordinator        string              `json:"coordinator"`
	CommitMode         string              `json:"commitMode"`
	AssignmentStrategy string              `json:"assignmentStrategy"`
}

// PartitionLagExt 分区延迟扩展信息
type PartitionLagExt struct {
	Topic                 string  `json:"topic"`
	Partition             int     `json:"partition"`
	CurrentOffset         int64   `json:"currentOffset"`
	LatestOffset          int64   `json:"latestOffset"`
	Lag                   int64   `json:"lag"`
	FirstMessageID        int64   `json:"firstMessageId"`
	LastMessageID         int64   `json:"lastMessageId"`
	TotalMessageCount     int64   `json:"totalMessageCount"`
	PartitionSizeBytes    int64   `json:"partitionSizeBytes"`
	ConsumedMessages      int64   `json:"consumedMessages"`
	RemainingMessages     int64   `json:"remainingMessages"`
	ConsumedPercentage    float64 `json:"consumedPercentage"`
	Metadata              string  `json:"metadata,omitempty"`
	UpdatedAt             string  `json:"updatedAt,omitempty"`
	GenerationID          int     `json:"generationId,omitempty"`
	InitialTopicWatermark *int64  `json:"initialTopicWatermark,omitempty"`
}

// ==================== Manual Assignment API Types ====================

// CreateManualAssignmentReq 创建手动分区分配请求
type CreateManualAssignmentReq struct {
	GroupID           string `json:"group_id" binding:"required"`            // 消费组 ID
	ConsumerIDPattern string `json:"consumer_id_pattern" binding:"required"` // 消费者 ID 模式
	Topic             string `json:"topic" binding:"required"`               // Topic 名称
	Partition         uint   `json:"partition"`                              // 分区号
}

// ManualAssignmentResp 手动分区分配响应
type ManualAssignmentResp struct {
	ID                int64     `json:"id"`
	GroupID           string    `json:"group_id"`
	ConsumerIDPattern string    `json:"consumer_id_pattern"`
	Topic             string    `json:"topic"`
	Partition         uint      `json:"partition"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// ListManualAssignmentsReq 查询手动分区分配列表请求
type ListManualAssignmentsReq struct {
	GroupID string `form:"group_id" binding:"required"` // 消费组 ID
}

// ==================== Dashboard API Types ====================

// DashboardDataResp 仪表板数据响应
type DashboardDataResp struct {
	Topics         []TopicResp         `json:"topics"`
	ConsumerGroups []ConsumerGroupResp `json:"consumerGroups"`
	System         SystemInfoResp      `json:"system"`
	Timestamp      string              `json:"timestamp"`
}

// SystemInfoResp 系统信息响应
type SystemInfoResp struct {
	Uptime  float64 `json:"uptime"`
	Version string  `json:"version"`
}

// DBMQStatsResp DBMQ 统计信息响应
type DBMQStatsResp struct {
	Cluster ClusterMetricsResp `json:"cluster"`
	Broker  BrokerResp         `json:"broker"`
	System  SystemInfoResp     `json:"system"`
}
