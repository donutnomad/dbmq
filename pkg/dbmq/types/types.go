package types

import (
	"database/sql"
	"time"
)

// Topic corresponds to the mq_topics table.
type Topic struct {
	TopicName      string `gorm:"primaryKey"`
	PartitionCount uint   `gorm:"not null"`
	// Configs stores topic-level configurations as a JSON string.
	Configs   sql.NullString `gorm:"type:json"`
	CreatedAt time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3)"`
}

func (t *Topic) TableName() string {
	return "mq_topics"
}

// Message corresponds to the mq_messages table. It holds the core message data.
type Message struct {
	ID         int64          `gorm:"primaryKey;autoIncrement"`
	Topic      string         `gorm:"not null"`
	Partition  uint           `gorm:"not null"`
	MessageKey sql.NullString `gorm:"index"`
	Headers    []byte         `gorm:"type:json"`
	Body       []byte         `gorm:"not null"`
	CreatedAt  time.Time      `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index:idx_consume_pull,priority:3"`
}

func (m *Message) TableName() string {
	return "mq_messages"
}

// ConsumerGroupGeneration corresponds to the mq_consumer_group_generations table.
// It tracks the generation of a consumer group, which increments with each rebalance.
type ConsumerGroupGeneration struct {
	GroupID      string `gorm:"primaryKey"`
	GenerationID uint   `gorm:"not null"`
	ProtocolType string `gorm:"not null;default:'consumer'"`
	LeaderID     string
	UpdatedAt    time.Time `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"`
}

func (c *ConsumerGroupGeneration) TableName() string {
	return "mq_consumer_group_generations"
}

// ConsumerHeartbeat corresponds to the mq_consumer_heartbeats table.
// It stores the liveness and assignments for each consumer in a group.
type ConsumerHeartbeat struct {
	GroupID            string    `gorm:"primaryKey"`
	ConsumerID         string    `gorm:"primaryKey"`
	GenerationID       uint      `gorm:"not null"`
	SubscribedTopics   []byte    `gorm:"type:json;not null"`
	AssignedPartitions []byte    `gorm:"type:json;not null"`
	LastHeartbeat      time.Time `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3);index"`
}

func (c *ConsumerHeartbeat) TableName() string {
	return "mq_consumer_heartbeats"
}

// ConsumerGroupOffset corresponds to the mq_consumer_group_offsets table.
// It stores the committed offset for a partition by a consumer group.
type ConsumerGroupOffset struct {
	GroupID         string `gorm:"primaryKey"`
	Topic           string `gorm:"primaryKey"`
	Partition       uint   `gorm:"primaryKey"`
	CommittedOffset int64  `gorm:"not null"`
	GenerationID    uint   `gorm:"not null"`
	Metadata        string
	UpdatedAt       time.Time `gorm:"type:timestamp(3);not null;default:CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)"`
}

func (c *ConsumerGroupOffset) TableName() string {
	return "mq_consumer_group_offsets"
}

// PartitionInfo uniquely identifies a topic-partition pair.
// It's used as a key in maps for managing offsets and assignments.
type PartitionInfo struct {
	Topic     string
	Partition uint
}

// ProducerMessage is the message to be sent by the producer.
type ProducerMessage struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// SendResult is the metadata for a record that has been successfully sent.
type SendResult struct {
	Topic     string
	Partition uint
	Offset    int64
}

// ConsumerMessage is a message received by the consumer from a poll request.
type ConsumerMessage struct {
	Topic     string
	Partition uint
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string]string
	Timestamp time.Time
}
