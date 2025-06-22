package errors

import "fmt"

// ErrCoordinatorNotLeader is returned when a request is sent to a coordinator that is not the current leader.
type ErrCoordinatorNotLeader struct {
	LeaderHint string // Provides a hint to who the actual leader might be.
}

func (e *ErrCoordinatorNotLeader) Error() string {
	return fmt.Sprintf("coordinator not leader, current leader hint: %s", e.LeaderHint)
}

// ErrIllegalGeneration is returned when a consumer sends a request with an outdated generation ID.
// This is a fatal error for the consumer, which must be fenced off.
type ErrIllegalGeneration struct {
	GroupID          string
	ConsumerID       string
	SentGeneration   uint
	ServerGeneration uint
}

func (e *ErrIllegalGeneration) Error() string {
	return fmt.Sprintf("illegal generation for consumer %s in group %s. sent: %d, server: %d",
		e.ConsumerID, e.GroupID, e.SentGeneration, e.ServerGeneration)
}

// ErrRebalanceInProgress is returned to a consumer when it tries to operate while a rebalance is occurring.
// The consumer should handle this by re-joining the group.
type ErrRebalanceInProgress struct {
	GroupID string
}

func (e *ErrRebalanceInProgress) Error() string {
	return fmt.Sprintf("rebalance in progress for group %s, consumer must rejoin", e.GroupID)
}

// ErrUnknownTopicOrPartition is returned when an operation targets a non-existent topic or partition.
type ErrUnknownTopicOrPartition struct {
	Topic     string
	Partition uint
}

func (e *ErrUnknownTopicOrPartition) Error() string {
	return fmt.Sprintf("unknown topic or partition: %s-%d", e.Topic, e.Partition)
}
