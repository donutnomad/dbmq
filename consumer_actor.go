package dbmq

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/donutnomad/dbmq/internal/db"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	repoLib "github.com/donutnomad/dbmq/internal/repo"
	"github.com/donutnomad/dbmq/logger"
)

// firstMessageId 数据库的消息ID主键是从1开始的
const firstMessageId = int64(1)

// ConsumerActor 管理 Consumer 所有状态的 Actor
// 所有状态修改都在单一 goroutine 中执行，无需锁
type ConsumerActor struct {
	// 配置（不可变）
	config ConsumerConfig
	id     string

	// 命令通道
	cmdCh chan Command

	// 状态（只在 actor 内访问，无锁）
	stateMachine             *consumerStateMachine
	generationID             uint
	assignment               []db.PartitionInfo
	alreadyConsumeMessageIDs map[db.PartitionInfo]int64
	offsetsToCommit          map[db.PartitionInfo]int64
	topics                   []string

	// 依赖（接口）
	repo     ConsumerRepo
	clock    Clock
	notifier Notifier
	waiter   Waiter // 等待接口，封装通知或超时等待逻辑

	// 生命周期
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ConsumerActorOption 定义 ConsumerActor 的可选配置函数
type ConsumerActorOption func(*ConsumerActor)

// WithRepo 设置自定义的数据访问层实现
// 主要用于单元测试时注入 mock 实现
func WithRepo(repo ConsumerRepo) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.repo = repo
	}
}

// WithClock 设置自定义的时钟实现
// 主要用于单元测试时注入 FakeClock 以控制时间
func WithClock(clock Clock) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.clock = clock
	}
}

// WithNotifier 设置自定义的通知器实现
// 主要用于单元测试时注入 FakeNotifier
func WithNotifier(notifier Notifier) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.notifier = notifier
	}
}

// WithWaiter 设置自定义的等待器实现
func WithWaiter(waiter Waiter) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.waiter = waiter
	}
}

// NewConsumerActor 创建一个新的 ConsumerActor
// 可选参数 opts 用于自定义配置，如注入自定义的 Repo、Clock、Notifier 实现
func NewConsumerActor(config ConsumerConfig, opts ...ConsumerActorOption) *ConsumerActor {
	// 如果启用了自动提交但没有设置间隔，使用默认值5秒
	if config.EnableAutoCommit && config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}

	actor := &ConsumerActor{
		config:                   config,
		id:                       utils.GenerateConsumerID(config.ClientID),
		cmdCh:                    make(chan Command, 100), // 带缓冲的命令通道
		stateMachine:             newConsumerStateMachine(),
		assignment:               nil,
		alreadyConsumeMessageIDs: make(map[db.PartitionInfo]int64),
		offsetsToCommit:          make(map[db.PartitionInfo]int64),
		topics:                   config.Topics,
		clock:                    NewRealClock(),
		stopCh:                   make(chan struct{}),
	}

	// 如果配置了数据库，创建默认 repo
	if config.DB != nil {
		actor.repo = repoLib.NewMqRepo(config.DB)
	}

	// 如果配置了 Redis 且启用了通知，创建默认 notifier
	if config.NotificationEnabled && config.Redis != nil {
		actor.notifier = NewRedisNotifier(config.Redis)
	}

	// 应用可选配置
	for _, opt := range opts {
		opt(actor)
	}

	// 设置默认 waiter：如果有 notifier 则用 notifier，否则用 NoopWaiter
	if actor.waiter == nil {
		if actor.notifier != nil {
			actor.waiter = actor.notifier
		} else {
			actor.waiter = NoopWaiter{}
		}
	}

	// 验证消费策略
	switch config.ConsumeStrategy {
	case ConsumeFromEarliest, ConsumeFromLatest:
		// 有效的策略
	default:
		panic(fmt.Errorf("未知的消费策略: %v", config.ConsumeStrategy))
	}

	// 设置状态转换日志回调
	actor.stateMachine.SetOnTransition(func(from, to ConsumerState) {
		actor.logger().Debug("状态转换",
			"group-id", config.GroupID,
			"consumer-id", actor.id,
			"from", from.String(),
			"to", to.String(),
		)
	})

	return actor
}

// Start 启动 actor 主循环
// 启动后 actor 处于 Uninitialized 状态，需要调用 SubscribeTopics 开始消费
func (a *ConsumerActor) Start() {
	a.wg.Add(1)
	go a.run()
}

// Stop 停止 actor
// 会等待所有后台 goroutine 结束
func (a *ConsumerActor) Stop() {
	select {
	case <-a.stopCh:
		// 已经关闭
		return
	default:
		close(a.stopCh)
	}
	a.wg.Wait()

	// 关闭 notifier
	if a.notifier != nil {
		if err := a.notifier.Close(); err != nil {
			a.logger().Warn("关闭通知器失败", "error", err, "consumer-id", a.id)
		}
	}
}

// run 是 actor 的主循环
// 所有状态修改都在此 goroutine 中执行，保证线程安全
func (a *ConsumerActor) run() {
	defer a.wg.Done()

	for {
		select {
		case cmd := <-a.cmdCh:
			a.handleCommand(cmd)
		case <-a.stopCh:
			a.logger().Debug("Actor 主循环收到停止信号", "consumer-id", a.id)
			return
		}
	}
}

// sendResult 发送结果到 channel 并关闭
func sendResult[R any](ch chan<- R, result R) {
	defer close(ch)
	ch <- result
}

// handleCommand 分发命令到对应的处理方法
func (a *ConsumerActor) handleCommand(cmd Command) {
	switch c := cmd.(type) {
	case HeartbeatCmd:
		sendResult(c.Result(), a.handleHeartbeat(c.context()))
	case PollCmd:
		sendResult(c.Result(), a.handlePoll(c.context(), c.Timeout))
	case RebalanceCmd:
		sendResult(c.Result(), a.handleRebalance(c.context(), c.NewGeneration, c.NewPartitions))
	case CloseCmd:
		sendResult(c.Result(), a.handleClose())
	case GetStateCmd:
		sendResult(c.Result(), a.handleGetState())
	case SubscribeCmd:
		sendResult(c.Result(), a.handleSubscribe(c.Topics))
	case CommitCmd:
		sendResult(c.Result(), a.handleCommit(c.context()))
	case UpdateOffsetsCmd:
		sendResult(c.Result(), a.handleUpdateOffsets(c.Offsets))
	default:
		a.logger().Warn("收到未知命令类型", "type", fmt.Sprintf("%T", cmd))
	}
}

// heartbeatLoop 心跳循环，定期发送 HeartbeatCmd 到 actor
func (a *ConsumerActor) heartbeatLoop() {
	defer a.wg.Done()

	ticker := a.clock.NewTicker(a.config.GetHeartbeatInterval())
	defer ticker.Stop()

	for {
		// 发送心跳命令
		cmd := NewHeartbeatCmd(context.Background())
		select {
		case a.cmdCh <- cmd:
			// 等待结果，失败时记录日志（下次心跳会重试）
			select {
			case err := <-cmd.ResultChan():
				if err != nil {
					a.logger().Warn("心跳处理失败", "error", err, "consumer-id", a.id)
				}
			case <-a.stopCh:
				return
			}
		case <-a.stopCh:
			return
		}

		// 等待下一次心跳
		select {
		case <-ticker.C():
		case <-a.stopCh:
			return
		}
	}
}

// autoCommitLoop 自动提交循环，定期提交偏移量
func (a *ConsumerActor) autoCommitLoop() {
	defer a.wg.Done()

	ticker := a.clock.NewTicker(a.config.AutoCommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C():
			cmd := NewCommitCmd(context.Background())
			select {
			case a.cmdCh <- cmd:
				select {
				case err := <-cmd.ResultChan():
					if err != nil {
						a.logger().Error("自动提交失败", "error", err, "consumer-id", a.id)
					}
				case <-a.stopCh:
					return
				}
			case <-a.stopCh:
				return
			}
		case <-a.stopCh:
			// 停止时不需要在这里提交，handleClose 已经负责最终提交
			a.logger().Debug("自动提交循环收到停止信号", "consumer-id", a.id)
			return
		}
	}
}

// ========== 命令处理方法 ==========

// handleHeartbeat 处理心跳命令
func (a *ConsumerActor) handleHeartbeat(ctx context.Context) error {
	// 如果消费者正在停止，不发送心跳
	if a.stateMachine.IsAny(StateStopping, StateStopped) {
		return nil
	}

	// 注册/更新心跳
	if err := a.repo.UpsertConsumerHeartbeat(ctx, a.config.GroupID, a.id, a.topics); err != nil {
		return errors.Wrap(err, "upsert heartbeat")
	}

	// 从数据库获取我们自己的状态
	hb, err := a.repo.GetConsumerHeartbeat(ctx, a.config.GroupID, a.id)
	if err != nil {
		return errors.Wrap(err, "get heartbeat")
	}
	if hb == nil {
		return nil
	}

	// 检查是否需要重新均衡
	if hb.GenerationID == a.generationID {
		return nil
	}

	// 执行重平衡
	return a.doRebalance(ctx, hb.GenerationID, slices.Clone(hb.AssignedPartitions))
}

// handlePoll 处理 Poll 命令
// 从分配的分区中拉取消息
func (a *ConsumerActor) handlePoll(ctx context.Context, timeout time.Duration) PollResult {
	// 检查状态
	currentState := a.stateMachine.Get()
	switch currentState {
	case StateReady:
		// 正常情况，继续执行
	case StateRebalancing, StateJoining:
		return PollResult{Err: &ErrRebalanceInProgress{GroupID: a.config.GroupID}}
	case StateStopping, StateStopped:
		return PollResult{Err: context.Canceled}
	default:
		return PollResult{Err: &ErrRebalanceInProgress{GroupID: a.config.GroupID}}
	}

	// 获取当前分配的分区
	assignedPartitions := a.assignment
	if len(assignedPartitions) == 0 {
		return PollResult{}
	}

	snapshotGeneration := a.generationID

	// 等待通知或超时
	if err := a.waitForNotification(ctx, timeout, assignedPartitions); err != nil {
		return PollResult{Err: err}
	}

	// 检查状态和 generation 是否变化
	if !a.stateMachine.IsReady() || a.generationID != snapshotGeneration {
		return PollResult{Err: &ErrRebalanceInProgress{GroupID: a.config.GroupID}}
	}

	// 批量获取消息
	fetchCtx, cancel := context.WithTimeout(ctx, a.config.GetPollFetchTimeout())
	defer cancel()

	requests := make([]repoLib.PartitionRequest, len(assignedPartitions))
	for i, p := range assignedPartitions {
		requests[i] = repoLib.PartitionRequest{
			Topic:     p.Topic,
			Partition: p.Partition,
			ID:        a.alreadyConsumeMessageIDs[p],
			Limit:     a.config.GetPollFetchLimit(),
		}
	}

	allMessages, err := a.repo.FetchMessagesBatch(fetchCtx, requests)
	if err != nil {
		if stderrors.Is(err, context.DeadlineExceeded) || stderrors.Is(err, context.Canceled) {
			return PollResult{Err: err}
		}
		return PollResult{Err: &ErrFailedFetchMessage{err}}
	}

	messages := new(ConsumerMessages).FromMessages(allMessages)
	return PollResult{Messages: messages}
}

// waitForNotification 等待通知或超时
func (a *ConsumerActor) waitForNotification(ctx context.Context, timeout time.Duration, _ []db.PartitionInfo) error {
	select {
	case <-a.waiter.Wait(ctx, timeout):
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-a.stopCh:
			return context.Canceled
		default:
			return nil
		}
	case <-a.stopCh:
		return context.Canceled
	}
}

// handleRebalance 处理重平衡命令
func (a *ConsumerActor) handleRebalance(ctx context.Context, newGeneration uint, newPartitions []db.PartitionInfo) error {
	return a.doRebalance(ctx, newGeneration, newPartitions)
}

// doRebalance 执行重平衡逻辑
func (a *ConsumerActor) doRebalance(ctx context.Context, newGeneration uint, newPartitions []db.PartitionInfo) error {
	prevState := a.stateMachine.Get()

	// 状态转换到 Rebalancing
	if err := a.stateMachine.Transition(StateRebalancing); err != nil {
		return errors.Wrapf(err, "state transition failed: %s -> %s", prevState, StateRebalancing)
	}

	oldPartitions := a.assignment

	// 清理旧状态并获取新分配的偏移量
	if err := a.clearAndFetchOffsetsForNewAssignment(ctx, newGeneration, newPartitions); err != nil {
		// 恢复到之前的状态，以便下次心跳可以重新触发重平衡
		if transErr := a.stateMachine.Transition(prevState); transErr != nil {
			a.logger().Warn("恢复状态失败", "error", transErr, "from", StateRebalancing, "to", prevState, "consumer-id", a.id)
		}
		return errors.Wrap(err, "fetch offsets for new assignment")
	}

	// 处理通知订阅（失败不阻止重平衡，但记录日志）
	if a.notifier != nil {
		if err := a.notifier.Unsubscribe(oldPartitions); err != nil {
			a.logger().Warn("取消订阅旧分区失败", "error", err, "consumer-id", a.id)
		}
		if err := a.notifier.Subscribe(newPartitions); err != nil {
			a.logger().Warn("订阅新分区失败", "error", err, "consumer-id", a.id)
		}
	}

	// 状态转换到 Ready
	if err := a.stateMachine.Transition(StateReady); err != nil {
		return errors.Wrapf(err, "state transition failed: %s -> %s", StateRebalancing, StateReady)
	}

	return nil
}

// clearAndFetchOffsetsForNewAssignment 清除旧状态并获取新分配的已提交偏移量
func (a *ConsumerActor) clearAndFetchOffsetsForNewAssignment(ctx context.Context, newGenerationID uint, newPartitions []db.PartitionInfo) error {
	// 计算被撤销的分区（在 oldPartitions 中但不在 newPartitions 中）
	revokedPartitions := utils.Subtract(a.assignment, newPartitions)

	// 获取已提交的偏移量
	fetchedOffsets, err := a.repo.GetCommittedOffsets(ctx, a.config.GroupID, newPartitions)
	if err != nil {
		return errors.Wrap(err, "get committed offsets")
	}
	fetchedOffsetsMap := fetchedOffsets.ToMap()

	// 筛选出新增的分区
	var addedPartitions []db.PartitionInfo
	for _, p := range newPartitions {
		if _, exists := fetchedOffsetsMap[p]; !exists {
			addedPartitions = append(addedPartitions, p)
		}
	}

	// 为新增分区确定起始消息ID
	partitionMaxIDMap := a.determineStartMessageID(ctx, addedPartitions)
	initialProgressWithWatermarks := make(map[db.PartitionInfo]repoLib.ConsumptionProgressWithWatermark)
	for k, startID := range partitionMaxIDMap {
		initialProgressWithWatermarks[k] = repoLib.ConsumptionProgressWithWatermark{
			LastConsumedMessageID:      startID - 1,
			SubscriptionStartWatermark: startID,
		}
	}

	// 为新增分区注册订阅信息
	if len(initialProgressWithWatermarks) > 0 {
		if err := a.repo.BatchCommitOffsetsWithInitialWatermark(ctx, a.config.GroupID, newGenerationID, initialProgressWithWatermarks); err != nil {
			return errors.Wrap(err, "batch commit initial watermark")
		}
	}

	// 清理被撤销分区的状态
	for _, p := range revokedPartitions {
		delete(a.offsetsToCommit, p)
		delete(a.alreadyConsumeMessageIDs, p)
	}

	// 更新新增分区的消费位置
	for k, v := range initialProgressWithWatermarks {
		a.alreadyConsumeMessageIDs[k] = v.LastConsumedMessageID
	}

	// 更新已有分区的消费位置
	for _, item := range fetchedOffsets {
		a.alreadyConsumeMessageIDs[db.PartitionInfo{Topic: item.Topic, Partition: item.Partition}] = item.LastConsumedMessageID
	}

	// 更新 generation 和分配
	a.generationID = newGenerationID
	a.assignment = newPartitions

	return nil
}

// determineStartMessageID 根据消费策略确定起始消息ID
func (a *ConsumerActor) determineStartMessageID(ctx context.Context, partitions []db.PartitionInfo) map[db.PartitionInfo]int64 {
	ret := make(map[db.PartitionInfo]int64)

	var needFetchFromDB []db.PartitionInfo
	for _, partition := range partitions {
		ret[partition] = firstMessageId
		if a.config.ConsumeStrategy == ConsumeFromLatest {
			needFetchFromDB = append(needFetchFromDB, partition)
		}
	}

	if len(needFetchFromDB) > 0 {
		byPartitions, err := a.repo.GetTopicsLatestIDsByPartitions(ctx, needFetchFromDB)
		if err == nil {
			maps.Copy(ret, byPartitions)
		}
	}

	return ret
}

// handleClose 处理关闭命令
func (a *ConsumerActor) handleClose() error {
	// 状态转换到 Stopping
	if err := a.stateMachine.Transition(StateStopping); err != nil {
		return nil // 已经在停止中，直接返回
	}

	// 如果是自动提交模式，执行最后一次提交
	if a.config.EnableAutoCommit {
		if err := a.commitOffsetsInternal(context.Background()); err != nil {
			a.logger().Warn("关闭时提交偏移量失败", "error", err, "consumer-id", a.id)
		}
	}

	// 标记消费者为离线
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := a.repo.MarkConsumerOffline(ctx, a.config.GroupID, a.id); err != nil {
		a.logger().Warn("标记消费者离线失败", "error", err, "consumer-id", a.id)
	}

	// 状态转换到 Stopped（从 Stopping -> Stopped 一定合法，不需要检查错误）
	a.stateMachine.MustTransition(StateStopped)

	return nil
}

// handleGetState 处理获取状态命令
func (a *ConsumerActor) handleGetState() GetStateResult {
	return GetStateResult{
		State:        a.stateMachine.Get(),
		GenerationID: a.generationID,
		Assignment:   slices.Clone(a.assignment),
	}
}

// handleSubscribe 处理订阅命令
func (a *ConsumerActor) handleSubscribe(topics []string) SubscribeResult {
	a.topics = topics

	// 状态转换: Uninitialized -> Joining
	if a.stateMachine.TransitionIf(StateUninitialized, StateJoining) {
		// 启动心跳循环
		a.wg.Add(1)
		go a.heartbeatLoop()

		// 如果启用了自动提交，启动自动提交循环
		if a.config.EnableAutoCommit {
			a.wg.Add(1)
			go a.autoCommitLoop()
		}

		return SubscribeResult{Started: true}
	}

	return SubscribeResult{Started: false}
}

// handleCommit 处理提交命令
func (a *ConsumerActor) handleCommit(ctx context.Context) error {
	return a.commitOffsetsInternal(ctx)
}

// commitOffsetsInternal 内部提交逻辑
func (a *ConsumerActor) commitOffsetsInternal(ctx context.Context) error {
	if len(a.offsetsToCommit) == 0 {
		return nil
	}

	commitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := a.repo.BatchCommitLastConsumeMessageID(commitCtx, a.config.GroupID, a.generationID, a.offsetsToCommit); err != nil {
		return errors.Wrap(err, "batch commit offsets")
	}

	// 提交成功后，更新内存状态
	for partition, messageID := range a.offsetsToCommit {
		a.alreadyConsumeMessageIDs[partition] = max(messageID, a.alreadyConsumeMessageIDs[partition])
	}
	clear(a.offsetsToCommit)

	return nil
}

// handleUpdateOffsets 处理更新偏移量命令
func (a *ConsumerActor) handleUpdateOffsets(offsets map[db.PartitionInfo]int64) struct{} {
	for partition, offset := range offsets {
		a.offsetsToCommit[partition] = max(offset, a.offsetsToCommit[partition])
	}

	return struct{}{}
}

// ========== 公开 API 方法 ==========

// Poll 从订阅的 Topic 和分区中拉取消息
// 会返回的错误:
// - ErrFailedFetchMessage
// - ErrRebalanceInProgress
// - context.DeadlineExceeded
// - context.Canceled
func (a *ConsumerActor) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	cmd := NewPollCmd(ctx, timeout)
	result, err := sendCmd(a, cmd, cmd.ResultChan())
	if err != nil {
		return nil, err
	}
	return result.Messages, result.Err
}

// SubscribeTopics 注册消费者要监听的 Topic 列表
// 必须在第一次调用 Poll 之前调用，同时触发消费者加入消费组并开始心跳
func (a *ConsumerActor) SubscribeTopics(topics ...string) {
	cmd := NewSubscribeCmd(topics)
	_, _ = sendCmd(a, cmd, cmd.ResultChan())
}

// Close 优雅关闭消费者，停止所有循环并最后提交一次偏移量
// 此方法是幂等的，多次调用是安全的
func (a *ConsumerActor) Close() {
	// 先检查是否已经停止
	select {
	case <-a.stopCh:
		return
	default:
	}

	cmd := NewCloseCmd()
	_, _ = sendCmd(a, cmd, cmd.ResultChan())

	// 停止 actor
	a.Stop()
}

// CommitSync 同步提交所有当前分配分区的消费进度
// ctx 用于控制超时，因为提交涉及数据库操作
func (a *ConsumerActor) CommitSync(ctx context.Context) error {
	cmd := NewCommitCmd(ctx)
	result, err := sendCmd(a, cmd, cmd.ResultChan())
	if err != nil {
		return err
	}
	return result
}

// State 返回消费者当前的状态
func (a *ConsumerActor) State() ConsumerState {
	cmd := NewGetStateCmd()
	result, err := sendCmd(a, cmd, cmd.ResultChan())
	if err != nil {
		return StateStopped
	}
	return result.State
}

// ID 返回消费者 ID
func (a *ConsumerActor) ID() string {
	return a.id
}

// IsReady 如果消费者处于 Ready 状态且有分配的分区，返回 true
func (a *ConsumerActor) IsReady() bool {
	cmd := NewGetStateCmd()
	result, err := sendCmd(a, cmd, cmd.ResultChan())
	if err != nil {
		return false
	}
	if result.State != StateReady {
		return false
	}
	// 检查是否有分配的分区
	return len(result.Assignment) > 0
}

// Acknowledge 确认一批消息已经成功处理
// 这会将这批消息中最大的偏移量标记为准备提交
func (a *ConsumerActor) Acknowledge(messages ...ConsumerMessage) {
	if len(messages) == 0 {
		return
	}

	// 按分区分组消息
	offsets := make(map[db.PartitionInfo]int64)
	for _, msg := range messages {
		p := msg.PartitionInfo()
		offsets[p] = max(msg.ID, offsets[p])
	}

	cmd := NewUpdateOffsetsCmd(offsets)
	_, _ = sendCmd(a, cmd, cmd.ResultChan())
}

func (a *ConsumerActor) logger() *slog.Logger {
	return logger.GetLogger().With("component", "consumer-actor")
}
