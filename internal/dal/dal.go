package dal

import (
	"context"
	"dbmq/pkg/types"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

// FindActiveConsumers finds all consumers in a group that have sent a heartbeat within the timeout period.
func FindActiveConsumers(ctx context.Context, db *gorm.DB, groupID string, timeout time.Duration) ([]types.ConsumerHeartbeat, error) {
	var activeConsumers []types.ConsumerHeartbeat
	sql := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `last_heartbeat` > ?"
	err := db.WithContext(ctx).
		Raw(sql, groupID, time.Now().Add(-timeout)).
		Scan(&activeConsumers).Error
	return activeConsumers, err
}

// GetConsumerGroupGeneration retrieves the current generation metadata for a consumer group.
func GetConsumerGroupGeneration(ctx context.Context, db *gorm.DB, groupID string) (*types.ConsumerGroupGeneration, error) {
	var gen types.ConsumerGroupGeneration
	sql := "SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ?"
	err := db.WithContext(ctx).Raw(sql, groupID).Scan(&gen).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil // Not an error, the group might be new
		}
		return nil, err
	}
	return &gen, nil
}

// IncrementAndGetGenerationID atomically increments the generation ID for a group and returns the new value.
// If the group does not exist, it creates one.
func IncrementAndGetGenerationID(ctx context.Context, db *gorm.DB, groupID string) (uint, error) {
	var gen types.ConsumerGroupGeneration

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Use FOR UPDATE to lock the row
		err := tx.Raw("SELECT * FROM `mq_consumer_group_generations` WHERE `group_id` = ? FOR UPDATE", groupID).Scan(&gen).Error
		if err != nil {
			// If the record is not found, we create it.
			if errors.Is(err, gorm.ErrRecordNotFound) {
				gen = types.ConsumerGroupGeneration{
					GroupID:      groupID,
					GenerationID: 1, // Start with generation 1
					ProtocolType: "consumer",
					UpdatedAt:    time.Now(),
				}
				insertSQL := "INSERT INTO `mq_consumer_group_generations` (`group_id`, `generation_id`, `protocol_type`, `updated_at`) VALUES (?, ?, ?, ?)"
				if err := tx.Exec(insertSQL, gen.GroupID, gen.GenerationID, gen.ProtocolType, gen.UpdatedAt).Error; err != nil {
					return err
				}
				return nil // End transaction successfully
			}
			return err // Other DB error
		}

		// If found, increment generation ID
		gen.GenerationID++
		updateSQL := "UPDATE `mq_consumer_group_generations` SET `generation_id` = ? WHERE `group_id` = ?"
		return tx.Exec(updateSQL, gen.GenerationID, gen.GroupID).Error
	})

	if err != nil {
		return 0, err
	}
	return gen.GenerationID, nil
}

// UpdateAssignmentsInTx updates the partition assignments for multiple consumers within a single transaction.
// The assignments map is consumerID -> partition JSON.
func UpdateAssignmentsInTx(ctx context.Context, tx *gorm.DB, groupID string, generationID uint, assignments map[string][]byte) error {
	updateSQL := "UPDATE `mq_consumer_heartbeats` SET `generation_id` = ?, `assigned_partitions` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	for consumerID, partitionsJSON := range assignments {
		err := tx.WithContext(ctx).Exec(updateSQL, generationID, partitionsJSON, groupID, consumerID).Error
		if err != nil {
			return err
		}
	}
	return nil
}

// FindTopicsByNames finds all topics that match the given names.
func FindTopicsByNames(ctx context.Context, db *gorm.DB, topicNames []string) ([]types.Topic, error) {
	if len(topicNames) == 0 {
		return nil, nil
	}
	var topics []types.Topic
	sql := "SELECT * FROM `mq_topics` WHERE `topic_name` IN (?)"
	err := db.WithContext(ctx).Raw(sql, topicNames).Scan(&topics).Error
	return topics, err
}

// GetHeartbeat retrieves a single consumer's heartbeat record.
func GetHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) (*types.ConsumerHeartbeat, error) {
	var hb types.ConsumerHeartbeat
	sql := "SELECT * FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?"
	err := db.WithContext(ctx).Raw(sql, groupID, consumerID).Scan(&hb).Error
	if err != nil {
		return nil, err
	}
	return &hb, nil
}

// UpsertHeartbeat atomically creates or updates a consumer's heartbeat.
// It updates the last_heartbeat time and ensures the consumer's subscribed topics are current.
// This is the primary function used by the consumer's heartbeat loop.
func UpsertHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string, subscribedTopics []byte) error {
	sql := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, 0, ?, ?, ?) ON DUPLICATE KEY UPDATE `last_heartbeat` = VALUES(`last_heartbeat`)"
	return db.WithContext(ctx).Exec(sql,
		groupID,
		consumerID,
		subscribedTopics,
		[]byte("{}"), // Default to empty JSON object
		time.Now(),
	).Error
}

// GetConsumerAssignment retrieves the partition assignment for a single consumer.
// It is an alias for GetHeartbeat as the assignment is stored in the heartbeat record.
func GetConsumerAssignment(ctx context.Context, db *gorm.DB, groupID, consumerID string) (*types.ConsumerHeartbeat, error) {
	return GetHeartbeat(ctx, db, groupID, consumerID)
}

// FetchMessages fetches messages from a specific partition after a given offset.
func FetchMessages(ctx context.Context, db *gorm.DB, topic string, partition uint, offset int64, limit int) ([]types.Message, error) {
	var messages []types.Message
	sql := "SELECT * FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` > ? ORDER BY `id` ASC LIMIT ?"
	err := db.WithContext(ctx).
		Raw(sql, topic, partition, offset, limit).
		Scan(&messages).Error
	return messages, err
}

// GetCommittedOffsets gets the committed offsets for a set of partitions for a group.
// It returns a map of PartitionInfo to the committed offset. Partitions with no committed offset will be absent from the map.
func GetCommittedOffsets(ctx context.Context, db *gorm.DB, groupID string, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	results := make(map[types.PartitionInfo]int64)
	if len(partitions) == 0 {
		return results, nil
	}

	var offsets []types.ConsumerGroupOffset

	// Build OR clauses for each partition since GORM has issues with complex IN queries
	var conditions []string
	var args []interface{}
	args = append(args, groupID) // First argument for group_id

	for _, p := range partitions {
		conditions = append(conditions, "(`topic` = ? AND `partition` = ?)")
		args = append(args, p.Topic, p.Partition)
	}

	whereClause := "`group_id` = ? AND (" + strings.Join(conditions, " OR ") + ")"

	err := db.WithContext(ctx).
		Model(&types.ConsumerGroupOffset{}).
		Where(whereClause, args...).
		Find(&offsets).Error

	if err != nil {
		return nil, err
	}

	for _, offset := range offsets {
		p := types.PartitionInfo{Topic: offset.Topic, Partition: offset.Partition}
		results[p] = offset.CommittedOffset
	}

	return results, nil
}

// BatchCommitOffsets commits a batch of offsets for a consumer group in a single transaction.
func BatchCommitOffsets(ctx context.Context, db *gorm.DB, groupID string, generationID uint, offsets map[types.PartitionInfo]int64) error {
	if len(offsets) == 0 {
		return nil
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for p, offset := range offsets {
			if err := CommitOffset(ctx, tx, groupID, generationID, p, offset); err != nil {
				return err
			}
		}
		return nil
	})
}

// CommitOffset commits an offset for a single partition.
func CommitOffset(ctx context.Context, db *gorm.DB, groupID string, generationID uint, p types.PartitionInfo, offset int64) error {
	// The IF(VALUES(generation_id) >= generation_id, ...) clause is the key to fencing.
	// It prevents a consumer from a previous generation (with a smaller generation_id)
	// from overwriting the offset of a consumer from the current or a future generation.
	sql := "INSERT INTO `mq_consumer_group_offsets` (`group_id`, `topic`, `partition`, `committed_offset`, `generation_id`, `updated_at`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `committed_offset` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`committed_offset`), `committed_offset`), `generation_id` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`generation_id`), `generation_id`), `updated_at` = IF(VALUES(`generation_id`) >= `generation_id`, VALUES(`updated_at`), `updated_at`)"
	return db.WithContext(ctx).Exec(sql, groupID, p.Topic, p.Partition, offset, generationID, time.Now()).Error
}

// CreateMessage inserts a new message into the database.
// It uses GORM's Create method to ensure the message's ID is populated post-insert.
func CreateMessage(ctx context.Context, db *gorm.DB, msg *types.Message) error {
	return db.WithContext(ctx).Create(msg).Error
}

// RegisterConsumer creates or updates a consumer's registration, including its topic subscriptions.
// This should be called when a consumer starts or changes its subscriptions.
func RegisterConsumer(ctx context.Context, db *gorm.DB, heartbeat *types.ConsumerHeartbeat) error {
	// This operation ensures a consumer's record exists and its subscribed topics are up-to-date.
	sql := "INSERT INTO `mq_consumer_heartbeats` (`group_id`, `consumer_id`, `generation_id`, `subscribed_topics`, `assigned_partitions`, `last_heartbeat`) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `subscribed_topics` = VALUES(`subscribed_topics`), `generation_id` = VALUES(`generation_id`), `last_heartbeat` = VALUES(`last_heartbeat`)"
	return db.WithContext(ctx).Exec(sql,
		heartbeat.GroupID,
		heartbeat.ConsumerID,
		heartbeat.GenerationID,
		heartbeat.SubscribedTopics,
		heartbeat.AssignedPartitions, // Initially empty
		heartbeat.LastHeartbeat,
	).Error
}

// UpdateHeartbeat updates only the last_heartbeat timestamp for a consumer.
// This is the lightweight operation that should be called periodically.
func UpdateHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) error {
	sql := "UPDATE `mq_consumer_heartbeats` SET `last_heartbeat` = ? WHERE `group_id` = ? AND `consumer_id` = ?"
	return db.WithContext(ctx).Exec(sql, time.Now(), groupID, consumerID).Error
}

// DeleteHeartbeat removes a consumer's heartbeat record entirely.
// This is used for a graceful shutdown, signaling an immediate leave from the group.
func DeleteHeartbeat(ctx context.Context, db *gorm.DB, groupID, consumerID string) error {
	sql := "DELETE FROM `mq_consumer_heartbeats` WHERE `group_id` = ? AND `consumer_id` = ?"
	return db.WithContext(ctx).Exec(sql, groupID, consumerID).Error
}

// FindAllActiveGroups finds all distinct group IDs that have sent a heartbeat within the timeout period.
func FindAllActiveGroups(ctx context.Context, db *gorm.DB, timeout time.Duration) ([]string, error) {
	var groupIDs []string
	sql := "SELECT DISTINCT `group_id` FROM `mq_consumer_heartbeats` WHERE `last_heartbeat` > ?"
	err := db.WithContext(ctx).
		Raw(sql, time.Now().Add(-timeout)).
		Pluck("group_id", &groupIDs).Error
	return groupIDs, err
}

// LowWatermark holds the result of the low watermark query.
type LowWatermark struct {
	Topic              string `gorm:"column:topic"`
	Partition          uint   `gorm:"column:partition"`
	LowWatermarkOffset int64  `gorm:"column:low_watermark"`
}

// GetConsumerGroupLowWatermarks calculates the minimum committed offset for every partition across all consumer groups.
// This is the "consumption low watermark".
func GetConsumerGroupLowWatermarks(ctx context.Context, db *gorm.DB) (map[types.PartitionInfo]int64, error) {
	var results []LowWatermark
	sql := "SELECT `topic`, `partition`, MIN(`committed_offset`) as low_watermark FROM `mq_consumer_group_offsets` GROUP BY `topic`, `partition`"

	err := db.WithContext(ctx).Raw(sql).Scan(&results).Error
	if err != nil {
		return nil, err
	}

	watermarks := make(map[types.PartitionInfo]int64, len(results))
	for _, res := range results {
		p := types.PartitionInfo{Topic: res.Topic, Partition: res.Partition}
		watermarks[p] = res.LowWatermarkOffset
	}

	return watermarks, nil
}

// GetAllTopics retrieves all topics from the database.
func GetAllTopics(ctx context.Context, db *gorm.DB) ([]types.Topic, error) {
	var topics []types.Topic
	err := db.WithContext(ctx).Find(&topics).Error
	return topics, err
}

// DeleteMessagesByPartition deletes messages from a partition that are older than a certain offset AND a certain time.
func DeleteMessagesByPartition(ctx context.Context, db *gorm.DB, topic string, partition uint, maxOffset int64, retentionDate time.Time, limit int) (int64, error) {
	// We must use a raw query because GORM does not support DELETE with table alias and JOIN.
	sql := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `id` < ? AND `created_at` < ? LIMIT ?"
	res := db.WithContext(ctx).Exec(sql, topic, partition, maxOffset, retentionDate, limit)
	return res.RowsAffected, res.Error
}

// DeleteMessagesByPartitionUnconsumed deletes messages from a partition that are older than a certain time.
// This is used for partitions that have no active consumers.
func DeleteMessagesByPartitionUnconsumed(ctx context.Context, db *gorm.DB, topic string, partition uint, retentionDate time.Time, limit int) (int64, error) {
	sql := "DELETE FROM `mq_messages` WHERE `topic` = ? AND `partition` = ? AND `created_at` < ? LIMIT ?"
	res := db.WithContext(ctx).Exec(sql, topic, partition, retentionDate, limit)
	return res.RowsAffected, res.Error
}
