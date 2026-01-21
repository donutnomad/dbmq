package db

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"gorm.io/datatypes"
)

func ApplySchemas(db interfaces.DB) error {
	return db.AutoMigrate(Topic{}, Message{}, ConsumerGroupGeneration{}, ConsumerHeartbeat{}, ConsumerGroupConsumptionProgress{}, ManualPartitionAssignment{})
}

// PartitionInfo 唯一标识一个Topic-分区对
// 在内存中用作Map的键，用于管理偏移量和分区分配
type PartitionInfo struct {
	Topic     string `json:"Topic"`     // Topic名称
	Partition uint   `json:"Partition"` // 分区号
}

func (p PartitionInfo) String() string {
	return fmt.Sprintf("(topic=%s,partition=%d)", p.Topic, p.Partition)
}

// Topic 元数据表
// 系统中最重要的表，存储所有消息数据
// 使用复合索引优化消费查询性能
type Topic struct {
	TopicName      string         `gorm:"primaryKey;type:varchar(255);column:topic_name;not null;"` // Topic名称
	PartitionCount uint           `gorm:"type:int unsigned;column:partition_count;not null;"`       // 分区数量，创建后不可修改
	Configs        datatypes.JSON `gorm:"type:json;column:configs;not null;"`                       // comment:'Topic级别配置, e.g. {"retention_ms": 604800000}'
	CreatedAt      time.Time      `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3)"`
}

// GetConfig 从Topic的JSON配置中获取指定配置项的值
// 返回值：配置值（float64类型）和是否找到该配置项的布尔值
func (t Topic) GetConfig(key string) (float64, bool) {
	var config map[string]any
	if err := json.Unmarshal(t.Configs, &config); err != nil {
		return 0, false // JSON格式无效
	}
	// 查找指定的配置项
	val, ok := config[key]
	if !ok {
		return 0, false
	}
	// JSON中的数字会被解析为float64类型
	if num, ok := val.(float64); ok {
		return num, true
	}
	return 0, false
}

func (Topic) TableName() string {
	return "mq_topics"
}

// Message 消息持久化日志表
type Message struct {
	CreatedAt  time.Time                             `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3);index:idx_created_at"`
	ID         int64                                 `gorm:"primaryKey;column:id;autoIncrement;index:idx_consume_pull,priority:3"` // 全局唯一ID，用于消息排序和消费
	Topic      string                                `gorm:"type:varchar(255);column:topic;not null;index:idx_consume_pull,priority:1"`
	Partition  uint                                  `gorm:"type:int unsigned;column:partition;not null;index:idx_consume_pull,priority:2"`
	MessageKey string                                `gorm:"type:varchar(255);column:message_key;not null"` // 消息的业务Key, 用于分区策略
	Headers    datatypes.JSONType[map[string]string] `gorm:"type:json;column:headers;not null"`
	Body       datatypes.JSON                        `gorm:"type:json;column:body;not null"`
}

func NewMessage(topic string, partition uint, messageKey string, headers map[string]string, body datatypes.JSON, createdAt time.Time) Message {
	return Message{
		CreatedAt:  createdAt,
		Topic:      topic,
		Partition:  partition,
		MessageKey: messageKey,
		Headers:    datatypes.NewJSONType(headers),
		Body:       body,
	}
}

func (m *Message) Fix() {
	if m.Body == nil {
		m.Body = []byte{} // 确保不为nil
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
}

func (m Message) ToPartitionInfo() PartitionInfo {
	return PartitionInfo{
		Topic:     m.Topic,
		Partition: m.Partition,
	}
}

func (Message) TableName() string {
	return "mq_messages"
}

// ConsumerGroupGeneration corresponds to the `mq_consumer_group_generations` table
// 消费组代际与元数据表
// 存储消费组的代际信息，是重新均衡机制的核心
// 每次重新均衡时代际ID递增，用于隔离不同代际的消费者
type ConsumerGroupGeneration struct {
	GroupID      string    `gorm:"primaryKey;type:varchar(255);column:group_id;not null;"` // 消费组ID
	GenerationID uint      `gorm:"type:int unsigned;column:generation_id;not null;"`       // 代际ID, 每次再均衡时加一
	ProtocolType string    `gorm:"type:varchar(50);column:protocol_type;not null;default:'consumer'"`
	LeaderID     string    `gorm:"type:varchar(255);column:leader_id;not null;default:''"`
	UpdatedAt    time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"`
}

func (ConsumerGroupGeneration) TableName() string {
	return "mq_consumer_group_generations"
}

// ConsumerHeartbeat corresponds to the `mq_consumer_heartbeats` table
// 消费者心跳与分区分配表
// 存储消费者心跳、分区分配和订阅信息
// 协调器通过此表判断消费者存活状态和进行分区分配
// last_heartbeat索引是性能关键，用于快速找到超时的消费者
// offline字段用于标识消费者是否已主动下线，避免删除历史记录
type ConsumerHeartbeat struct {
	GroupID            string                             `gorm:"primaryKey;type:varchar(255);column:group_id;not null"`
	ConsumerID         string                             `gorm:"primaryKey;type:varchar(255);column:consumer_id;not null;"`          // 消费者唯一ID (e.g., UUID)
	GenerationID       uint                               `gorm:"type:int unsigned;column:generation_id;not null;"`                   // 消费者当前所属的代际ID
	SubscribedTopics   datatypes.JSONSlice[string]        `gorm:"type:json;column:subscribed_topics;not null;"`                       // 订阅的Topic列表
	AssignedPartitions datatypes.JSONSlice[PartitionInfo] `gorm:"type:json;column:assigned_partitions;not null;"`                     // 被分配的分区
	Offline            bool                               `gorm:"column:;not null;default:false;index:idx_offline_status,priority:1"` // 是否已下线：true=主动下线，false=在线或超时
	LastHeartbeat      time.Time                          `gorm:"column:last_heartbeat;type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index:idx_last_heartbeat;index:idx_offline_status,priority:2"`
	OfflineAt          *time.Time                         `gorm:"column:offline_at;type:timestamp(3);"` // 下线时间，仅当offline=true时有效
}

func (ConsumerHeartbeat) TableName() string {
	return "mq_consumer_heartbeats"
}

// ConsumerGroupConsumptionProgress corresponds to the `mq_consumer_group_consumption_progress` table
// 消费组消费进度跟踪表
// 存储消费组对每个分区的消费进度和状态
// 实现"至少一次"消费语义的关键表
// 使用代际隔离防止旧代际消费者覆盖新代际的进度
// 新设计解决了offset命名混乱和手动提交模式下的注册问题
type ConsumerGroupConsumptionProgress struct {
	GroupID                    string    `gorm:"primaryKey;type:varchar(255);column:group_id;not null"`
	Topic                      string    `gorm:"primaryKey;type:varchar(255);column:topic;not null"`
	Partition                  uint      `gorm:"primaryKey;type:int unsigned;column:partition;not null"`
	LastConsumedMessageID      int64     `gorm:"column:last_consumed_message_id;not null;default:-1;"`                                       // 最后成功消费的消息ID，-1表示还未消费任何消息
	SubscriptionRegisteredAt   time.Time `gorm:"type:timestamp(3);column:subscription_registered_at;not null;default:CURRENT_TIMESTAMP(3);"` // 消费组首次订阅此分区的时间
	SubscriptionStartWatermark int64     `gorm:"column:subscription_start_watermark"`                                                        // 订阅时topic的最新消息ID，用于区分消费策略(从头开始/从最新开始)
	GenerationID               uint      `gorm:"type:int unsigned;column:generation_id;not null;"`                                           // 最后更新此记录时的代际ID，用于并发控制
	Metadata                   string    `gorm:"type:varchar(255);column:metadata;default:''"`                                               // 可选的元数据信息
	UpdatedAt                  time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"`
}

func (p ConsumerGroupConsumptionProgress) PartitionInfo() PartitionInfo {
	return PartitionInfo{
		Topic:     p.Topic,
		Partition: p.Partition,
	}
}

func (ConsumerGroupConsumptionProgress) TableName() string {
	return "mq_consumer_group_consumption_progress"
}

type ConsumerGroupConsumptionProgressSlice []ConsumerGroupConsumptionProgress

func (s ConsumerGroupConsumptionProgressSlice) ToMap() map[PartitionInfo]int64 {
	var results = make(map[PartitionInfo]int64)
	for _, progress := range s {
		results[PartitionInfo{Topic: progress.Topic, Partition: progress.Partition}] = progress.LastConsumedMessageID
	}
	return results
}

// ManualPartitionAssignment 手动分区分配配置
// 用于覆盖默认的自动分区分配策略，支持将特定分区固定分配给特定消费者
type ManualPartitionAssignment struct {
	ID                int64     `gorm:"primaryKey;column:id;autoIncrement"`                                                                      // 自增主键
	GroupID           string    `gorm:"type:varchar(255);column:group_id;not null;uniqueIndex:idx_unique_assignment,priority:1"`                 // 消费组ID
	ConsumerIDPattern string    `gorm:"type:varchar(255);column:consumer_id_pattern;not null;uniqueIndex:idx_unique_assignment,priority:2"`      // 消费者ID匹配模式
	Topic             string    `gorm:"type:varchar(255);column:topic;not null;uniqueIndex:idx_unique_assignment,priority:3"`                    // Topic名称
	Partition         uint      `gorm:"type:int unsigned;column:partition;not null;uniqueIndex:idx_unique_assignment,priority:4"`                // 分区号
	CreatedAt         time.Time `gorm:"type:timestamp(3);column:created_at;not null;default:CURRENT_TIMESTAMP(3)"`                               // 创建时间
	UpdatedAt         time.Time `gorm:"type:timestamp(3);column:updated_at;not null;default:CURRENT_TIMESTAMP(3);onUpdate:CURRENT_TIMESTAMP(3)"` // 更新时间
}

func (ManualPartitionAssignment) TableName() string {
	return "mq_manual_partition_assignments"
}
