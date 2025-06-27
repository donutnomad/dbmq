package types

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/datatypes"
)

// Topic 对应 mq_topics 表，存储Topic的元数据信息
// Topic是消息的逻辑分类，一个Topic可以包含多个分区
type Topic struct {
	TopicName      string         `gorm:"primaryKey"`                                              // Topic名称，作为主键，全局唯一
	PartitionCount uint           `gorm:"not null"`                                                // 分区数量，创建后不可修改，用于实现并行处理
	Configs        sql.NullString `gorm:"type:json"`                                               // Topic级别配置，JSON格式存储，如保留时间等
	CreatedAt      time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3)"` // 创建时间，精确到毫秒
}

// GetConfig 从Topic的JSON配置中获取指定配置项的值
// 返回值：配置值（float64类型）和是否找到该配置项的布尔值
func (t *Topic) GetConfig(key string) (float64, bool) {
	// 检查配置是否为空
	if !t.Configs.Valid {
		return 0, false
	}

	// 解析JSON配置
	var config map[string]interface{}
	if err := json.Unmarshal([]byte(t.Configs.String), &config); err != nil {
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

func (t *Topic) TableName() string {
	return "mq_topics"
}

// Message 对应 mq_messages 表，存储消息的核心数据
// 这是系统中最重要的表，存储了所有的消息内容
// 使用全局ID作为偏移量，消除分区内offset计算的死锁问题
type Message struct {
	ID         int64          `gorm:"primaryKey;autoIncrement"`                   // 全局唯一ID，用于消息排序和消费偏移量
	Topic      string         `gorm:"not null;index:idx_consume_pull,priority:1"` // 消息所属的Topic
	Partition  uint           `gorm:"not null;index:idx_consume_pull,priority:2"` // 消息所属的分区号
	MessageKey sql.NullString // 消息的业务Key，用于分区路由策略，可为空
	Headers    []byte         `gorm:"type:json"`                                                                    // 消息头，JSON格式存储键值对
	Body       []byte         `gorm:"not null"`                                                                     // 消息体，实际的消息内容
	CreatedAt  time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index:idx_created_at"` // 消息创建时间
}

func NewMessage(topic string, partition uint, messageKey []byte, headers map[string]string, body []byte, createdAt time.Time) Message {
	var messageKey_ sql.NullString
	var headers_ []byte
	// 设置消息Key（如果有）
	if len(messageKey) > 0 {
		messageKey_.String = string(messageKey)
		messageKey_.Valid = true
	}
	// 序列化消息头（如果有）
	if headers != nil {
		headersJSON, err := json.Marshal(headers)
		if err != nil {
			panic(fmt.Errorf("failed to marshal headers to json: %w", err))
		}
		headers_ = headersJSON
	}

	msg := Message{
		Topic:      topic,
		Partition:  partition,
		MessageKey: messageKey_,
		Headers:    headers_,
		Body:       body,
		CreatedAt:  createdAt,
	}

	return msg
}

func (m *Message) Fix() {
	if m.Headers == nil {
		m.Headers = []byte("null") // 确保不为nil
	}
	if m.Body == nil {
		m.Body = []byte{} // 确保不为nil
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now()
	}
}

func (m *Message) TableName() string {
	return "mq_messages"
}

// ConsumerGroupGeneration 对应 mq_consumer_group_generations 表
// 存储消费组的代际信息，每次重新均衡时代际ID会递增
// 代际机制是实现分布式协调和防止"脑裂"的关键
type ConsumerGroupGeneration struct {
	GroupID      string         `gorm:"primaryKey"`                  // 消费组ID，主键
	GenerationID uint           `gorm:"not null"`                    // 代际ID，每次重新均衡时递增，用于隔离不同代际的消费者
	ProtocolType string         `gorm:"not null;default:'consumer'"` // 协议类型，默认为consumer
	LeaderID     sql.NullString // 消费组领导者ID，用于分布式协调
	UpdatedAt    time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"` // 最后更新时间
}

func (c *ConsumerGroupGeneration) TableName() string {
	return "mq_consumer_group_generations"
}

// ConsumerHeartbeat 对应 mq_consumer_heartbeats 表
// 存储消费者的活跃状态、订阅信息和分区分配
// 这是协调器判断消费者是否存活和进行分区分配的核心表
type ConsumerHeartbeat struct {
	GroupID            string                             `gorm:"primaryKey"`                                                                       // 消费组ID，复合主键之一
	ConsumerID         string                             `gorm:"primaryKey"`                                                                       // 消费者唯一ID（通常是UUID），复合主键之一
	GenerationID       uint                               `gorm:"not null"`                                                                         // 消费者当前所属的代际ID，用于版本控制
	SubscribedTopics   datatypes.JSONSlice[string]        `gorm:"type:json;not null"`                                                               // 消费者订阅的Topic列表，JSON格式，如["topic-A", "topic-B"]
	AssignedPartitions datatypes.JSONSlice[PartitionInfo] `gorm:"type:json;not null"`                                                               // 协调器分配给消费者的分区，JSON格式，如{"topic-A": [0, 2]}
	Offline            bool                               `gorm:"not null;default:false"`                                                           // 是否已下线：true=主动下线，false=在线或超时
	LastHeartbeat      time.Time                          `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index:idx_last_heartbeat"` // 最后心跳时间，用于检测消费者是否存活
	OfflineAt          sql.NullTime                       `gorm:"type:timestamp(3)"`                                                                // 下线时间，仅当offline=true时有效
}

func (c *ConsumerHeartbeat) TableName() string {
	return "mq_consumer_heartbeats"
}

// ConsumerGroupOffset 对应 mq_consumer_group_offsets 表
// 存储消费组对每个分区的已提交偏移量
// 这是实现"至少一次"消费语义的关键，确保消息不会丢失
type ConsumerGroupOffset struct {
	GroupID               string         `gorm:"primaryKey"` // 消费组ID，复合主键之一
	Topic                 string         `gorm:"primaryKey"` // Topic名称，复合主键之一
	Partition             uint           `gorm:"primaryKey"` // 分区号，复合主键之一
	CommittedOffset       int64          `gorm:"not null"`   // 已提交的偏移量，指向下一条要消费的消息
	InitialTopicWatermark sql.NullInt64  // 消费组首次加入topic时的topic最新消息ID，用于区分消费策略
	GenerationID          uint           `gorm:"not null"` // 提交该偏移量时的代际ID，防止旧代际覆盖新代际的偏移量
	Metadata              sql.NullString // 可选的元数据信息
	UpdatedAt             time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"` // 最后更新时间
}

func (c *ConsumerGroupOffset) TableName() string {
	return "mq_consumer_group_offsets"
}

// PartitionInfo 唯一标识一个Topic-分区对
// 在内存中用作Map的键，用于管理偏移量和分区分配
type PartitionInfo struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
}

func (p PartitionInfo) String() string {
	return fmt.Sprintf("(topic=%s,partition=%d)", p.Topic, p.Partition)
}

// ProducerMessage 生产者要发送的消息结构
// 这是应用程序使用的消息格式，会被转换为数据库中的Message结构
type ProducerMessage struct {
	Topic   string            // 目标Topic
	Key     []byte            // 消息Key，用于分区路由，如果为空则使用轮询策略
	Value   []byte            // 消息内容
	Headers map[string]string // 消息头，键值对格式
}

// SendResult 消息发送成功后返回的元数据
// 包含了消息在系统中的精确位置信息
type SendResult struct {
	Topic     string // 消息所在的Topic
	Partition uint   // 消息所在的分区
	Offset    int64  // 消息的全局ID（即Message表的ID字段）
}

// ConsumerMessage 消费者从轮询请求中接收到的消息
// 这是应用程序接收到的消息格式，从数据库Message转换而来
type ConsumerMessage struct {
	Topic     string            // 消息所属的Topic
	Partition uint              // 消息所属的分区
	Offset    int64             // 消息的全局ID（用作偏移量）
	Key       []byte            // 消息Key
	Value     []byte            // 消息内容
	Headers   map[string]string // 消息头
	Timestamp time.Time         // 消息时间戳
}
