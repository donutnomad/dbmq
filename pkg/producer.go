package pkg

import (
	"context"
	dberrors "dbmq/pkg/errors"
	"dbmq/pkg/types"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"dbmq/internal/dal"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProducerConfig holds configuration for the producer.
type ProducerConfig struct {
	// NotificationEnabled enables the Redis real-time notification optimization.
	// This is a "fire-and-forget" operation.
	NotificationEnabled bool
	// NotificationStateTTL sets the expiration for the notification state key in Redis.
	// This prevents a lock-in if a consumer crashes before resetting the state.
	// Defaults to 60 seconds.
	NotificationStateTTL time.Duration
	DB                   *gorm.DB
	Redis                *redis.Client
}

// ProducerMessage is the message to be sent by the producer.
type ProducerMessage struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string]string
}

// SendResult is the metadata for a record that has been sent.
type SendResult struct {
	Topic     string
	Partition uint
	Offset    int64
}

const (
	// producerNotifyScript is a Lua script that implements the "Intelligent Notification Coalescing" logic.
	// It only sends a notification if the notification state key for a partition does not exist.
	//
	// KEYS[1]: The state key (e.g., "mq_notify_state:{topic}:{partition}")
	// KEYS[2]: The notification channel (e.g., "mq_notify:{topic}:{partition}")
	// ARGV[1]: The notification message (e.g., "new_message")
	// ARGV[2]: The expiration time for the state key in seconds.
	//
	// Returns 1 if the notification was sent, 0 if it was coalesced.
	producerNotifyScript = `
if redis.call("SET", KEYS[1], "notified", "NX", "EX", ARGV[2]) then
  redis.call("PUBLISH", KEYS[2], ARGV[1])
  return 1
else
  return 0
end
`
	defaultNotificationStateTTL = 60 * time.Second
)

var (
	notifyScript = redis.NewScript(producerNotifyScript)
)

// Producer is a message producer that is safe for concurrent use.
type Producer struct {
	config             ProducerConfig
	db                 *gorm.DB
	redis              *redis.Client
	topicMetadataCache sync.Map // map[string]*types.Topic
	roundRobinCounters sync.Map // map[string]*atomic.Uint32 for thread-safe counters
}

// NewProducer creates a new Producer instance.
func NewProducer(config ProducerConfig) (*Producer, error) {
	if config.NotificationStateTTL == 0 {
		config.NotificationStateTTL = defaultNotificationStateTTL
	}
	return &Producer{
		config:             config,
		db:                 config.DB,
		redis:              config.Redis,
		topicMetadataCache: sync.Map{},
		roundRobinCounters: sync.Map{},
	}, nil
}

// Send sends a message to a topic. This operation is blocking.
func (p *Producer) Send(ctx context.Context, msg *ProducerMessage) (*SendResult, error) {
	if msg == nil || msg.Topic == "" {
		return nil, errors.New("producer message and topic cannot be empty")
	}

	topicMeta, err := p.getTopicMetadata(ctx, msg.Topic)
	if err != nil {
		return nil, err
	}
	partitionCount := topicMeta.PartitionCount

	// Select partition
	var partition uint
	if msg.Key != nil && len(msg.Key) > 0 {
		partition = p.hashPartition(msg.Key, partitionCount)
	} else {
		partition = p.nextRoundRobinPartition(msg.Topic, partitionCount)
	}

	// Send the message to the database
	dbMsg := &types.Message{
		Topic:     msg.Topic,
		Partition: partition,
		Body:      msg.Value,
		CreatedAt: time.Now(),
	}

	if msg.Key != nil {
		dbMsg.MessageKey.String = string(msg.Key)
		dbMsg.MessageKey.Valid = true
	}
	if msg.Headers != nil {
		headersJSON, err := json.Marshal(msg.Headers)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal headers to json: %w", err)
		}
		dbMsg.Headers = headersJSON
	}

	if err := dal.CreateMessage(ctx, p.db, dbMsg); err != nil {
		return nil, fmt.Errorf("failed to create message in db: %w", err)
	}

	// Optionally, send an intelligent notification
	if p.config.NotificationEnabled && p.redis != nil {
		go p.sendNotification(context.Background(), msg.Topic, partition)
	}

	return &SendResult{
		Topic:     msg.Topic,
		Partition: partition,
		Offset:    dbMsg.ID,
	}, nil
}

func (p *Producer) sendNotification(ctx context.Context, topic string, partition uint) {
	stateKey := fmt.Sprintf("mq_notify_state:%s:%d", topic, partition)
	channelKey := fmt.Sprintf("mq_notify:%s:%d", topic, partition)
	keys := []string{stateKey, channelKey}
	args := []interface{}{"new_message", p.config.NotificationStateTTL.Seconds()}

	res, err := notifyScript.Run(ctx, p.redis, keys, args...).Result()
	if err != nil {
		// Log the error but don't fail the send operation
		fmt.Printf("Failed to run notification script for %s: %v\n", channelKey, err)
		return
	}

	// For debugging/logging purposes, you might want to know if a notification was sent or coalesced.
	// For example:
	if val, ok := res.(int64); ok && val == 1 {
		// fmt.Printf("Notification sent for %s\n", channelKey)
	} else {
		// fmt.Printf("Notification coalesced for %s\n", channelKey)
	}
}

func (p *Producer) getTopicMetadata(ctx context.Context, topicName string) (*types.Topic, error) {
	if metadata, ok := p.topicMetadataCache.Load(topicName); ok {
		return metadata.(*types.Topic), nil
	}

	topics, err := dal.FindTopicsByNames(ctx, p.db, []string{topicName})
	if err != nil {
		return nil, fmt.Errorf("failed to find topic '%s': %w", topicName, err)
	}
	if len(topics) == 0 {
		return nil, &dberrors.ErrUnknownTopicOrPartition{Topic: topicName}
	}

	topic := &topics[0]
	p.topicMetadataCache.Store(topicName, topic)
	return topic, nil
}

func (p *Producer) nextRoundRobinPartition(topic string, partitionCount uint) uint {
	if partitionCount == 0 {
		return 0
	}
	if partitionCount == 1 {
		return 0
	}
	counter, _ := p.roundRobinCounters.LoadOrStore(topic, &atomic.Uint32{})
	// Increment counter and wrap around if it exceeds partitionCount
	newVal := counter.(*atomic.Uint32).Add(1)
	return uint(newVal-1) % partitionCount
}

func (p *Producer) hashPartition(key []byte, partitionCount uint) uint {
	if partitionCount == 0 {
		return 0
	}
	hasher := fnv.New64a()
	hasher.Write(key)
	return uint(hasher.Sum64() % uint64(partitionCount))
}

// Close is a placeholder for future cleanup logic.
func (p *Producer) Close() {
	// No-op for now as DB/Redis connections are managed externally.
}
