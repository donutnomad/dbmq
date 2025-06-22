package errors

import "fmt"

// ErrCoordinatorNotLeader 当请求发送到不是当前领导者的协调器时返回此错误
// 这有助于客户端重定向到正确的领导者节点
type ErrCoordinatorNotLeader struct {
	LeaderHint string // 提供当前实际领导者的提示信息
}

func (e *ErrCoordinatorNotLeader) Error() string {
	return fmt.Sprintf("coordinator not leader, current leader hint: %s", e.LeaderHint)
}

// ErrIllegalGeneration 当消费者发送带有过时代际ID的请求时返回此错误
// 这是消费者的致命错误，必须被隔离
// 代际机制是防止"脑裂"和确保数据一致性的关键
type ErrIllegalGeneration struct {
	GroupID          string // 消费组ID
	ConsumerID       string // 消费者ID
	SentGeneration   uint   // 消费者发送的代际ID
	ServerGeneration uint   // 服务器当前的代际ID
}

func (e *ErrIllegalGeneration) Error() string {
	return fmt.Sprintf("illegal generation for consumer %s in group %s. sent: %d, server: %d",
		e.ConsumerID, e.GroupID, e.SentGeneration, e.ServerGeneration)
}

// ErrRebalanceInProgress 当消费者在重新均衡期间尝试操作时返回此错误
// 消费者应该通过重新加入消费组来处理此错误
// 这确保了重新均衡过程的原子性和数据一致性
type ErrRebalanceInProgress struct {
	GroupID string // 正在进行重新均衡的消费组ID
}

func (e *ErrRebalanceInProgress) Error() string {
	return fmt.Sprintf("rebalance in progress for group %s, consumer must rejoin", e.GroupID)
}

// ErrUnknownTopicOrPartition 当操作针对不存在的Topic或分区时返回此错误
// 这通常表示配置错误或Topic尚未创建
type ErrUnknownTopicOrPartition struct {
	Topic     string // Topic名称
	Partition uint   // 分区号
}

func (e *ErrUnknownTopicOrPartition) Error() string {
	return fmt.Sprintf("unknown topic or partition: %s-%d", e.Topic, e.Partition)
}
