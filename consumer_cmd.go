package dbmq

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq/internal/db"
)

// Command 表示发送给 Consumer Actor 的命令
type Command interface {
	isCommand()
	context() context.Context
}

// BaseCmd 命令基础结构体
type BaseCmd[R any] struct {
	ctx    context.Context
	result chan R
}

func NewBaseCmd[R any](ctx context.Context) BaseCmd[R] {
	return BaseCmd[R]{ctx: ctx, result: make(chan R, 1)}
}

func (BaseCmd[R]) isCommand()                 {}
func (c BaseCmd[R]) context() context.Context { return c.ctx }
func (c BaseCmd[R]) ResultChan() <-chan R     { return c.result }
func (c BaseCmd[R]) Result() chan<- R         { return c.result }

// ========== 命令定义 ==========

type HeartbeatCmd struct{ BaseCmd[error] }

func NewHeartbeatCmd(ctx context.Context) HeartbeatCmd {
	return HeartbeatCmd{BaseCmd: NewBaseCmd[error](ctx)}
}

type PollResult struct {
	Messages []ConsumerMessage
	Err      error
}
type PollCmd struct {
	BaseCmd[PollResult]
	Timeout time.Duration
}

func NewPollCmd(ctx context.Context, timeout time.Duration) PollCmd {
	return PollCmd{BaseCmd: NewBaseCmd[PollResult](ctx), Timeout: timeout}
}

type RebalanceCmd struct {
	BaseCmd[error]
	NewGeneration uint
	NewPartitions []db.PartitionInfo
}

func NewRebalanceCmd(ctx context.Context, newGeneration uint, newPartitions []db.PartitionInfo) RebalanceCmd {
	return RebalanceCmd{
		BaseCmd:       NewBaseCmd[error](ctx),
		NewGeneration: newGeneration,
		NewPartitions: newPartitions,
	}
}

type CloseCmd struct{ BaseCmd[error] }

func NewCloseCmd() CloseCmd {
	return CloseCmd{BaseCmd: NewBaseCmd[error](context.Background())}
}

type GetStateResult struct {
	State        ConsumerState
	GenerationID uint
	Assignment   []db.PartitionInfo
}
type GetStateCmd struct{ BaseCmd[GetStateResult] }

func NewGetStateCmd() GetStateCmd {
	return GetStateCmd{BaseCmd: NewBaseCmd[GetStateResult](context.Background())}
}

type SubscribeResult struct {
	Started bool
}
type SubscribeCmd struct {
	BaseCmd[SubscribeResult]
	Topics []string
}

func NewSubscribeCmd(topics []string) SubscribeCmd {
	return SubscribeCmd{BaseCmd: NewBaseCmd[SubscribeResult](context.Background()), Topics: topics}
}

type CommitCmd struct{ BaseCmd[error] }

func NewCommitCmd(ctx context.Context) CommitCmd {
	return CommitCmd{BaseCmd: NewBaseCmd[error](ctx)}
}

type UpdateOffsetsCmd struct {
	BaseCmd[struct{}]
	Offsets map[db.PartitionInfo]int64
}

func NewUpdateOffsetsCmd(offsets map[db.PartitionInfo]int64) UpdateOffsetsCmd {
	return UpdateOffsetsCmd{BaseCmd: NewBaseCmd[struct{}](context.Background()), Offsets: offsets}
}

// ========== 命令发送 ==========

// sendCmd 发送命令到 actor 并等待响应
func sendCmd[R any](a *ConsumerActor, cmd Command, resultCh <-chan R) (R, error) {
	ctx := cmd.context()

	select {
	case a.cmdCh <- cmd:
	case <-ctx.Done():
		var zero R
		return zero, ctx.Err()
	case <-a.stopCh:
		var zero R
		return zero, context.Canceled
	}

	select {
	case result := <-resultCh:
		return result, nil
	case <-ctx.Done():
		var zero R
		return zero, ctx.Err()
	case <-a.stopCh:
		var zero R
		return zero, context.Canceled
	}
}
