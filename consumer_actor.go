package dbmq

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/pkg/utils"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/types"
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
	assignment               []types.PartitionInfo
	alreadyConsumeMessageIDs map[types.PartitionInfo]int64
	offsetsToCommit          map[types.PartitionInfo]int64
	topics                   []string

	// 依赖（接口）
	heartbeatRepo heartbeat.Repo
	progressRepo  consumerprogress.Repo
	messageRepo   message.Repo
	clock         Clock
	notifier      Notifier
	waiter        Waiter // 等待接口，封装通知或超时等待逻辑

	// 生命周期
	stopCh          chan struct{}
	wg              sync.WaitGroup
	shutdownOnce    sync.Once
	startMu         sync.Mutex
	started         bool
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc
	pollPermit      chan struct{}
	rebalanceMu     sync.Mutex
	rebalanceCh     chan struct{}

	// 回调准入：deliveryMu 仅保护以下字段，不在 onMessage 执行期间持有，
	// 因此回调内同步调用 Close/Stop 不会自锁（见 waitDeliveriesIdle 的重入处理）。
	deliveryMu     sync.Mutex
	deliveryCount  int
	deliveryIdle   chan struct{} // 计数归零时关闭，供 shutdown 等待
	deliveringGIDs map[int64]int // 正在执行 onMessage 的 goroutine，用于识别重入 Close
}

// ConsumerActorOption 定义 ConsumerActor 的可选配置函数
type ConsumerActorOption func(*ConsumerActor)

// WithHeartbeatRepo 设置自定义的心跳仓储实现
func WithHeartbeatRepo(repo heartbeat.Repo) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.heartbeatRepo = repo
	}
}

// WithProgressRepo 设置自定义的消费进度仓储实现
func WithProgressRepo(repo consumerprogress.Repo) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.progressRepo = repo
	}
}

// WithMessageRepo 设置自定义的消息仓储实现
func WithMessageRepo(repo message.Repo) ConsumerActorOption {
	return func(a *ConsumerActor) {
		a.messageRepo = repo
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
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	pollPermit := make(chan struct{}, 1)
	pollPermit <- struct{}{}

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
		alreadyConsumeMessageIDs: make(map[types.PartitionInfo]int64),
		offsetsToCommit:          make(map[types.PartitionInfo]int64),
		topics:                   config.Topics,
		clock:                    NewRealClock(),
		stopCh:                   make(chan struct{}),
		lifecycleCtx:             lifecycleCtx,
		lifecycleCancel:          lifecycleCancel,
		pollPermit:               pollPermit,
		rebalanceCh:              make(chan struct{}),
	}

	// 如果配置了数据库，创建默认 repo
	if config.DB != nil {
		actor.heartbeatRepo = heartbeatrepo.New(config.DB)
		actor.progressRepo = consumerprogressrepo.New(config.DB)
		actor.messageRepo = messagerepo.New(config.DB)
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
	a.startMu.Lock()
	defer a.startMu.Unlock()
	if a.started || a.lifecycleCtx.Err() != nil {
		return
	}
	a.started = true
	a.wg.Add(1)
	go a.run()
}

// Stop 优雅停止 actor，语义与 Close 一致。
// 会等待已开始的 onMessage 回调和所有后台 goroutine 结束。
func (a *ConsumerActor) Stop() {
	a.shutdown()
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
	case PollSnapshotCmd:
		sendResult(c.Result(), a.handlePollSnapshot())
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
		cmd := NewHeartbeatCmd(a.lifecycleCtx)
		result, err := sendCmd(a, cmd, cmd.ResultChan())
		if err != nil {
			if a.lifecycleCtx.Err() != nil {
				return
			}
			a.logger().Warn("心跳命令失败", "error", err, "consumer-id", a.id)
		} else if result != nil {
			a.logger().Warn("心跳处理失败", "error", result, "consumer-id", a.id)
		}

		select {
		case <-a.lifecycleCtx.Done():
			return
		default:
		}

		// 等待下一次心跳
		select {
		case <-ticker.C():
		case <-a.lifecycleCtx.Done():
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
			cmd := NewCommitCmd(a.lifecycleCtx)
			result, err := sendCmd(a, cmd, cmd.ResultChan())
			if err != nil {
				if a.lifecycleCtx.Err() != nil {
					return
				}
				a.logger().Error("自动提交命令失败", "error", err, "consumer-id", a.id)
			} else if result != nil {
				a.logger().Error("自动提交失败", "error", result, "consumer-id", a.id)
			}
			select {
			case <-a.lifecycleCtx.Done():
				return
			default:
			}
		case <-a.lifecycleCtx.Done():
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

	heartbeatCtx, cancel := context.WithTimeout(ctx, a.config.GetHeartbeatOperationTimeout())
	defer cancel()

	// 注册/更新心跳
	if err := a.heartbeatRepo.Upsert(heartbeatCtx, a.config.GroupID, a.id, a.topics); err != nil {
		return errors.Wrap(err, "upsert heartbeat")
	}

	// 从数据库获取我们自己的状态
	hb, err := a.heartbeatRepo.Get(heartbeatCtx, a.config.GroupID, a.id)
	if err != nil {
		return errors.Wrap(err, "get heartbeat")
	}
	if hb == nil {
		a.logger().Warn("心跳写入后读取失败", "consumer-id", a.id)
		return nil
	}

	// 检查 generation 变化
	currentGen := a.generationID
	dbGen := hb.GenerationID

	if dbGen == currentGen {
		if utils.SameElements(hb.AssignedPartitions, a.assignment) {
			return nil
		}
		// 同代际但 assignment 与数据库不一致（手动修数据或未知异常），触发自愈
		a.logger().Warn("generation 相同但 assignment 与数据库不一致，触发自愈重平衡",
			"consumer-id", a.id, "generation", dbGen,
			"db-partitions", len(hb.AssignedPartitions), "local-partitions", len(a.assignment))
		return a.doRebalance(ctx, dbGen, hb.AssignedPartitions)
	}
	if dbGen < currentGen {
		// 心跳行可能被删除后以 gen=0 重建，或组被删除重建。仍采纳数据库状态
		// （安全：宁可暂停消费），协调器的 generation 落后检测会很快推进行 generation
		a.logger().Warn("检测到 generation 回退，采纳数据库状态",
			"consumer-id", a.id, "current-generation", currentGen, "db-generation", dbGen)
	}

	// Debug 日志
	a.logger().Debug("检测到 generation 变化，触发重新均衡",
		"consumer-id", a.id,
		"current-generation", currentGen,
		"new-generation", dbGen,
		"current-state", a.stateMachine.Get().String(),
	)

	// 执行重平衡
	return a.doRebalance(ctx, hb.GenerationID, hb.AssignedPartitions)
}

// handlePollSnapshot 返回 Poll 流程所需的状态快照（纯读，快速返回）
// Assignment 与 Offsets 在同一条命令内克隆，保证两者一致
func (a *ConsumerActor) handlePollSnapshot() PollSnapshotResult {
	return PollSnapshotResult{
		State:        a.stateMachine.Get(),
		GenerationID: a.generationID,
		Assignment:   slices.Clone(a.assignment),
		Offsets:      maps.Clone(a.alreadyConsumeMessageIDs),
	}
}

// checkPollable 校验快照状态是否允许 Poll
func checkPollable(state ConsumerState, groupID string) error {
	switch state {
	case StateReady:
		return nil
	case StateStopping, StateStopped:
		return context.Canceled
	default: // Uninitialized/Joining/Rebalancing
		return &ErrRebalanceInProgress{GroupID: groupID}
	}
}

// pollSnapshot 获取 actor 状态快照
func (a *ConsumerActor) pollSnapshot(ctx context.Context) (PollSnapshotResult, error) {
	cmd := NewPollSnapshotCmd(ctx)
	return sendCmd(a, cmd, cmd.ResultChan())
}

// currentRebalanceCh 返回当前代际的 rebalance 广播 channel。
func (a *ConsumerActor) currentRebalanceCh() <-chan struct{} {
	a.rebalanceMu.Lock()
	defer a.rebalanceMu.Unlock()
	return a.rebalanceCh
}

// lifecycleDone 返回 actor 生命周期结束信号。
func (a *ConsumerActor) lifecycleDone() <-chan struct{} {
	return a.lifecycleCtx.Done()
}

// deliver 串起回调准入与 shutdown：shutdown 返回后不会有新回调开始。
// 回调执行期间不持有任何锁，因此 onMessage 内同步调用 Close/Stop 是安全的
// （此时 shutdown 检测到重入，跳过等待自身回调，不会死锁）。
func (a *ConsumerActor) deliver(messages []ConsumerMessage, onMessage func([]ConsumerMessage)) bool {
	a.deliveryMu.Lock()
	if a.lifecycleCtx.Err() != nil {
		a.deliveryMu.Unlock()
		return false
	}
	a.deliveryCount++
	// 标记本 goroutine 正在回调中，供 shutdown 识别重入调用。
	gid := goroutineID()
	if a.deliveringGIDs == nil {
		a.deliveringGIDs = make(map[int64]int)
	}
	a.deliveringGIDs[gid]++
	a.deliveryMu.Unlock()

	defer func() {
		a.deliveryMu.Lock()
		a.deliveryCount--
		if a.deliveringGIDs[gid] <= 1 {
			delete(a.deliveringGIDs, gid)
		} else {
			a.deliveringGIDs[gid]--
		}
		if a.deliveryCount == 0 && a.deliveryIdle != nil {
			close(a.deliveryIdle)
			a.deliveryIdle = nil
		}
		a.deliveryMu.Unlock()
	}()

	onMessage(messages)
	return true
}

// waitDeliveriesIdle 等待所有进行中的 onMessage 回调结束。
// 若调用方自身就在回调中（重入 Close/Stop），则不等待自己，避免自锁。
func (a *ConsumerActor) waitDeliveriesIdle() {
	gid := goroutineID()

	a.deliveryMu.Lock()
	// 重入：调用方自身就在 onMessage 中，等待自己必然死锁，直接返回。
	if a.deliveringGIDs[gid] > 0 {
		a.deliveryMu.Unlock()
		return
	}
	if a.deliveryCount == 0 {
		a.deliveryMu.Unlock()
		return
	}
	if a.deliveryIdle == nil {
		a.deliveryIdle = make(chan struct{})
	}
	idle := a.deliveryIdle
	a.deliveryMu.Unlock()

	<-idle
}

func (a *ConsumerActor) shutdown() {
	a.shutdownOnce.Do(func() {
		// 禁止新的 Poll 和回调，并等待已开始的回调完成。
		// lifecycleCancel 必须早于取 startMu：Start 在 startMu 内检查 lifecycleCtx，
		// 二者共同保证下方 started==false 分支不会与 run goroutine 并发写状态机。
		a.lifecycleCancel()
		a.waitDeliveriesIdle()

		a.startMu.Lock()
		started := a.started
		a.startMu.Unlock()
		if started {
			cmd := NewCloseCmd()
			_, _ = sendCmd(a, cmd, cmd.ResultChan())
		} else if a.stateMachine.Transition(StateStopping) == nil {
			a.stateMachine.MustTransition(StateStopped)
		}

		close(a.stopCh)
		a.wg.Wait()

		if a.notifier != nil {
			if err := a.notifier.Close(); err != nil {
				a.logger().Warn("关闭通知器失败", "error", err, "consumer-id", a.id)
			}
		}
	})
}

// contextWithLifecycle 将调用方 context 与 actor 生命周期绑定。
func (a *ConsumerActor) contextWithLifecycle(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}

	pollCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(a.lifecycleCtx, cancel)
	if a.lifecycleCtx.Err() != nil {
		cancel()
	}
	return pollCtx, func() {
		stop()
		cancel()
	}
}

func (a *ConsumerActor) acquirePoll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.pollPermit:
		return nil
	}
}

func (a *ConsumerActor) releasePoll() {
	a.pollPermit <- struct{}{}
}

// signalRebalance 唤醒当前所有等待中的 Poll，并为下一代创建新 channel。
func (a *ConsumerActor) signalRebalance() {
	a.rebalanceMu.Lock()
	close(a.rebalanceCh)
	a.rebalanceCh = make(chan struct{})
	a.rebalanceMu.Unlock()
}

// fetchMessages 在调用者 goroutine 中按快照批量拉取消息
func (a *ConsumerActor) fetchMessages(ctx context.Context, snap PollSnapshotResult) ([]ConsumerMessage, error) {
	fetchCtx, cancel := context.WithTimeout(ctx, a.config.GetPollFetchTimeout())
	defer cancel()

	requests := make([]message.FetchRequest, len(snap.Assignment))
	for i, p := range snap.Assignment {
		requests[i] = message.FetchRequest{
			Topic:     p.Topic,
			Partition: p.Partition,
			AfterID:   snap.Offsets[p],
			Limit:     a.config.GetPollFetchLimit(),
		}
	}

	allMessages, err := a.messageRepo.FetchBatch(fetchCtx, requests)
	if err != nil {
		if parentErr := ctx.Err(); parentErr != nil {
			return nil, parentErr
		}
		return nil, &ErrFailedFetchMessage{err}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return new(ConsumerMessages).FromDomainMessages(allMessages), nil
}

// waitForNotification 等待通知、超时或 rebalance 广播。
func (a *ConsumerActor) waitForNotification(ctx context.Context, timeout time.Duration, rebalanceCh <-chan struct{}) error {
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	select {
	case <-a.waiter.Wait(waitCtx, timeout):
		if ctx.Err() != nil {
			return ctx.Err()
		}
		select {
		case <-a.stopCh:
			return context.Canceled
		default:
			return nil
		}
	case <-rebalanceCh:
		return nil
	case <-a.stopCh:
		return context.Canceled
	}
}

// handleRebalance 处理重平衡命令
func (a *ConsumerActor) handleRebalance(ctx context.Context, newGeneration uint, newPartitions []types.PartitionInfo) error {
	return a.doRebalance(ctx, newGeneration, newPartitions)
}

// rebalanceTimeout 重平衡的 DB 操作超时上限。
// 限制 DB 无响应时的 actor 阻塞时间；失败后进入 Joining，下次心跳会重试。
const rebalanceTimeout = 15 * time.Second

// doRebalance 执行重平衡逻辑
func (a *ConsumerActor) doRebalance(ctx context.Context, newGeneration uint, newPartitions []types.PartitionInfo) error {
	ctx, cancel := context.WithTimeout(ctx, rebalanceTimeout)
	defer cancel()

	prevState := a.stateMachine.Get()
	isInitialJoin := prevState == StateJoining

	// 状态转换到 Rebalancing
	if err := a.stateMachine.Transition(StateRebalancing); err != nil {
		return errors.Wrapf(err, "state transition failed: %s -> %s", prevState, StateRebalancing)
	}
	a.signalRebalance()

	oldPartitions := a.assignment

	// 清理旧状态并获取新分配的偏移量
	if err := a.clearAndFetchOffsetsForNewAssignment(ctx, newGeneration, newPartitions); err != nil {
		// 保持暂停消费，下次心跳继续重试数据库中的新分配。
		if transErr := a.stateMachine.Transition(StateJoining); transErr != nil {
			a.logger().Warn("进入重试状态失败", "error", transErr, "from", StateRebalancing, "to", StateJoining, "consumer-id", a.id)
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

	// 区分初始化完成和普通重新均衡
	if isInitialJoin {
		a.logger().Info("✅ 消费者初始化完成",
			"consumer-id", a.id,
			"group-id", a.config.GroupID,
			"generation-id", a.generationID,
			"assigned-partitions", len(a.assignment),
		)
	} else {
		a.logger().Info("消费者重新均衡完成",
			"consumer-id", a.id,
			"generation-id", a.generationID,
			"old-partitions", len(oldPartitions),
			"new-partitions", len(a.assignment),
		)
	}

	return nil
}

// clearAndFetchOffsetsForNewAssignment 清除旧状态并获取新分配的已提交偏移量
func (a *ConsumerActor) clearAndFetchOffsetsForNewAssignment(ctx context.Context, newGenerationID uint, newPartitions []types.PartitionInfo) error {
	// 计算被撤销的分区（在 oldPartitions 中但不在 newPartitions 中）
	revokedPartitions := utils.Subtract(a.assignment, newPartitions)

	// 获取已提交的偏移量
	fetchedOffsets, err := a.progressRepo.GetCommittedOffsets(ctx, a.config.GroupID, newPartitions)
	if err != nil {
		return errors.Wrap(err, "get committed offsets")
	}
	fetchedOffsetsMap := make(map[types.PartitionInfo]int64)
	for _, p := range fetchedOffsets {
		fetchedOffsetsMap[types.PartitionInfo{Topic: p.Topic, Partition: p.Partition}] = p.LastConsumedMessageID
	}

	// 筛选出新增的分区
	var addedPartitions []types.PartitionInfo
	for _, p := range newPartitions {
		if _, exists := fetchedOffsetsMap[p]; !exists {
			addedPartitions = append(addedPartitions, p)
		}
	}

	// 为新增分区确定起始消息ID
	partitionMaxIDMap, err := a.determineStartMessageID(ctx, addedPartitions)
	if err != nil {
		return err
	}
	initialProgressWithWatermarks := make(map[types.PartitionInfo]consumerprogress.ProgressWithWatermark)
	for k, startID := range partitionMaxIDMap {
		initialProgressWithWatermarks[k] = consumerprogress.ProgressWithWatermark{
			LastConsumedMessageID:      startID - 1,
			SubscriptionStartWatermark: startID,
		}
	}

	// 为新增分区注册订阅信息
	if len(initialProgressWithWatermarks) > 0 {
		if err := a.progressRepo.BatchCommitOffsetsWithWatermark(ctx, a.config.GroupID, newGenerationID, initialProgressWithWatermarks); err != nil {
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
		a.alreadyConsumeMessageIDs[types.PartitionInfo{Topic: item.Topic, Partition: item.Partition}] = item.LastConsumedMessageID
	}

	// 更新 generation 和分配
	a.generationID = newGenerationID
	a.assignment = newPartitions

	return nil
}

// determineStartMessageID 根据消费策略确定起始消息ID
func (a *ConsumerActor) determineStartMessageID(ctx context.Context, partitions []types.PartitionInfo) (map[types.PartitionInfo]int64, error) {
	ret := make(map[types.PartitionInfo]int64)

	var needFetchFromDB []types.PartitionInfo
	for _, partition := range partitions {
		ret[partition] = firstMessageId
		if a.config.ConsumeStrategy == ConsumeFromLatest {
			needFetchFromDB = append(needFetchFromDB, partition)
		}
	}

	if len(needFetchFromDB) > 0 {
		byPartitions, err := a.messageRepo.GetLatestIDs(ctx, needFetchFromDB)
		if err != nil {
			return nil, errors.Wrap(err, "get latest message IDs")
		}
		maps.Copy(ret, byPartitions)
	}

	return ret, nil
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
	if err := a.heartbeatRepo.MarkOffline(ctx, a.config.GroupID, a.id); err != nil {
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

	if err := a.progressRepo.BatchCommitOffsets(commitCtx, a.config.GroupID, a.generationID, a.offsetsToCommit); err != nil {
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
func (a *ConsumerActor) handleUpdateOffsets(offsets map[types.PartitionInfo]int64) struct{} {
	for partition, offset := range offsets {
		a.offsetsToCommit[partition] = max(offset, a.offsetsToCommit[partition])
	}

	return struct{}{}
}

// ========== 公开 API 方法 ==========

// Poll 从订阅的 Topic 和分区中拉取消息
// 等待与拉取在调用者 goroutine 中执行，不会阻塞 actor 主循环——
// 否则心跳命令会排队等待长达整个 Poll timeout，导致消费者被协调器误判死亡。
// 每个 ConsumerActor 同时执行一个 Poll；并发调用会等待当前 Poll 完成。
// 会返回的错误:
// - ErrFailedFetchMessage（包括内部数据库查询超时；应优先使用 errors.As 分类）
// - ErrRebalanceInProgress
// - context.DeadlineExceeded（调用方 context 到期，且未匹配 ErrFailedFetchMessage）
// - context.Canceled（调用方取消或 Consumer 关闭，且未匹配 ErrFailedFetchMessage）
func (a *ConsumerActor) Poll(ctx context.Context, timeout time.Duration) ([]ConsumerMessage, error) {
	pollCtx, releaseLifecycle := a.contextWithLifecycle(ctx)
	defer releaseLifecycle()
	if err := a.acquirePoll(pollCtx); err != nil {
		return nil, err
	}
	defer a.releasePoll()

	if err := pollCtx.Err(); err != nil {
		return nil, err
	}

	// 在快照前捕获 channel，覆盖 rebalance 发生在 channel 读取与快照之间的竞态。
	rebalanceCh := a.currentRebalanceCh()

	// 阶段1：快照校验状态，空分区立即返回
	snap1, err := a.pollSnapshot(pollCtx)
	if err != nil {
		return nil, err
	}
	if err := checkPollable(snap1.State, a.config.GroupID); err != nil {
		return nil, err
	}
	if len(snap1.Assignment) == 0 {
		return nil, nil
	}

	// 阶段2：在调用者 goroutine 中等待通知或超时
	// （a.waiter 构造后不可变、a.stopCh 仅 close，线程安全）
	if err := a.waitForNotification(pollCtx, timeout, rebalanceCh); err != nil {
		return nil, err
	}

	// 阶段3：重新快照。fetch 必须使用 snap2 的 Assignment+Offsets：
	// - 等待期间可能发生同代际 assignment 自愈（见 handleHeartbeat），仅比较 generation 查不出
	// - 等待期间 commit 会推进 offsets，使用 snap1 的会重复拉取
	snap2, err := a.pollSnapshot(pollCtx)
	if err != nil {
		return nil, err
	}
	if err := checkPollable(snap2.State, a.config.GroupID); err != nil {
		return nil, err
	}
	if snap2.GenerationID != snap1.GenerationID {
		return nil, &ErrRebalanceInProgress{GroupID: a.config.GroupID}
	}
	if len(snap2.Assignment) == 0 {
		return nil, nil
	}

	// 阶段4：在调用者 goroutine 中拉取消息。
	// 快照通过后到 FetchBatch 之间存在极小窗口可能拉到刚被撤销分区的消息，
	// 与旧实现（fetch 后排队的 rebalance 同样在应用处理消息前执行）暴露等价，
	// at-least-once 语义下可接受
	return a.fetchMessages(pollCtx, snap2)
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
	a.shutdown()
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
	offsets := make(map[types.PartitionInfo]int64)
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
