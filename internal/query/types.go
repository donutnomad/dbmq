package query

import "time"

// =============================================================================
// Cluster DTOs
// =============================================================================

// ClusterStats 集群统计数据
type ClusterStats struct {
	TopicCount      int       // Topic 总数
	PartitionCount  int       // 分区总数
	MessageCount    int64     // 消息总数
	ConsumerGroups  int       // 消费组数量
	ActiveConsumers int       // 活跃消费者数量
	Timestamp       time.Time // 统计时间戳
}

// BrokerStats Broker 统计数据
type BrokerStats struct {
	TopicCount     int   // Topic 数量
	PartitionCount int   // 分区数量
	MessageCount   int64 // 消息总数
}

// =============================================================================
// Topic DTOs
// =============================================================================

// TopicStats Topic 统计数据
type TopicStats struct {
	TotalMessages int64            // 消息总数
	TotalSize     int64            // 存储总大小
	LatestOffset  int64            // 最新偏移量
	Partitions    []PartitionStats // 分区详情
}

// PartitionStats 分区统计数据
type PartitionStats struct {
	Partition      uint   // 分区号
	FirstMessageID int64  // 最早消息 ID
	LastMessageID  int64  // 最新消息 ID
	MessageCount   int64  // 消息数量
	SizeBytes      int64  // 存储大小
	CreatedAt      string // 最早消息时间
	UpdatedAt      string // 最新消息时间
}

// =============================================================================
// Consumer Group DTOs
// =============================================================================

// PartitionLag 分区延迟信息
type PartitionLag struct {
	Topic                      string  // Topic 名称
	Partition                  uint    // 分区号
	CurrentOffset              int64   // 当前已提交偏移量
	LatestOffset               int64   // 最新消息偏移量
	Lag                        int64   // 延迟数量
	SubscriptionStartWatermark int64   // 订阅时的水位线
	TotalMessageCount          int64   // 分区总消息数
	ConsumedMessages           int64   // 已消费消息数
	RemainingMessages          int64   // 剩余消息数
	ConsumedPercentage         float64 // 消费进度百分比
	UpdatedAt                  int64   // 更新时间戳 (毫秒)
}

// ConsumerGroupExtended 消费组扩展信息
type ConsumerGroupExtended struct {
	GenerationID       int                      // 代际 ID
	LeaderID           string                   // Leader ID
	UpdatedAt          time.Time                // 更新时间
	Members            []ConsumerMemberExtended // 成员列表
	SubscribedProgress []SubscribedProgress     // 消费进度列表
}

// ConsumerMemberExtended 消费者成员扩展信息
type ConsumerMemberExtended struct {
	ConsumerID         string     // 消费者 ID
	GenerationID       int        // 代际 ID
	SubscribedTopics   string     // 订阅的 Topic 列表 (JSON)
	AssignedPartitions string     // 分配的分区 (JSON)
	Offline            bool       // 是否离线
	LastHeartbeat      time.Time  // 最后心跳时间
	OfflineAt          *time.Time // 离线时间
}

// SubscribedProgress 订阅的消费进度
type SubscribedProgress struct {
	Topic                      string    // Topic 名称
	Partition                  int       // 分区号
	CommittedOffset            int64     // 已提交偏移量
	GenerationID               int       // 代际 ID
	Metadata                   string    // 元数据
	UpdatedAt                  time.Time // 更新时间
	SubscriptionStartWatermark *int64    // 订阅时的水位线
}

// =============================================================================
// Message DTOs
// =============================================================================

// MessageSearchRequest 消息搜索请求
type MessageSearchRequest struct {
	Topic     string     // Topic 名称
	Partition *uint      // 分区号 (可选)
	FromTime  *time.Time // 开始时间 (可选)
	ToTime    *time.Time // 结束时间 (可选)
	Search    string     // 搜索关键字 (可选)
	Offset    int64      // 偏移量
	Limit     int        // 每页数量
}

// MessageSearchResult 消息搜索结果
type MessageSearchResult struct {
	Messages []MessageRecord // 消息列表
	Total    int64           // 总数
}

// MessageRecord 消息记录
type MessageRecord struct {
	ID         int64             // 消息 ID
	Topic      string            // Topic 名称
	Partition  uint              // 分区号
	MessageKey string            // 消息 Key
	Body       []byte            // 消息体
	Headers    map[string]string // 消息头
	CreatedAt  time.Time         // 创建时间
}
