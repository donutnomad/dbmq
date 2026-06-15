package dbmq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/logger"
	"github.com/redis/go-redis/v9"
)

// Consumer 代表一个消费者实例，属于某个消费组
// 消费者负责从分配的分区中拉取消息、处理消息、提交偏移量
// 支持自动重新均衡和故障恢复
//
// 内部实现基于 Actor 模型，所有状态修改都在单一 goroutine 中执行
type Consumer struct {
	config ConsumerConfig        // 消费者配置
	redis  redis.UniversalClient // Redis连接（可选）

	// Actor 实现
	actor *ConsumerActor
}

// ConsumerOption 定义 Consumer 的可选配置函数
type ConsumerOption func(*ConsumerActor)

// WithConsumerRepos 设置自定义的数据访问层实现
// 主要用于单元测试时注入 mock 实现
func WithConsumerRepos(heartbeatRepo heartbeat.Repo, progressRepo consumerprogress.Repo, messageRepo message.Repo) ConsumerOption {
	return func(a *ConsumerActor) {
		a.heartbeatRepo = heartbeatRepo
		a.progressRepo = progressRepo
		a.messageRepo = messageRepo
	}
}

// WithConsumerClock 设置自定义的时钟实现
// 主要用于单元测试时注入 FakeClock 以控制时间
func WithConsumerClock(clock Clock) ConsumerOption {
	return func(a *ConsumerActor) {
		a.clock = clock
	}
}

// WithConsumerNotifier 设置自定义的通知器实现
// 主要用于单元测试时注入 FakeNotifier
func WithConsumerNotifier(notifier Notifier) ConsumerOption {
	return func(a *ConsumerActor) {
		a.notifier = notifier
	}
}

// NewConsumer 创建一个新的消费者实例
// 可选参数 opts 用于自定义配置，如注入自定义的 Repo 实现
func NewConsumer(config ConsumerConfig, opts ...ConsumerOption) (*Consumer, error) {
	// 如果启用了自动提交但没有设置间隔，使用默认值5秒
	if config.EnableAutoCommit && config.AutoCommitInterval == 0 {
		config.AutoCommitInterval = 5 * time.Second
	}

	// 创建 Actor，使用自定义配置
	actorOpts := make([]ConsumerActorOption, 0, len(opts)+2)

	// 默认设置
	if config.DB != nil {
		actorOpts = append(actorOpts,
			WithHeartbeatRepo(heartbeatrepo.New(config.DB)),
			WithProgressRepo(consumerprogressrepo.New(config.DB)),
			WithMessageRepo(messagerepo.New(config.DB)),
		)
	}
	if config.NotificationEnabled && config.Redis != nil {
		actorOpts = append(actorOpts, WithNotifier(NewRedisNotifier(config.Redis)))
	}

	// 将 ConsumerOption 转换为 ConsumerActorOption
	for _, opt := range opts {
		actorOpts = append(actorOpts, ConsumerActorOption(opt))
	}

	actor := NewConsumerActor(config, actorOpts...)

	consumer := &Consumer{
		config: config,
		redis:  config.Redis,
		actor:  actor,
	}

	// 启动 actor
	actor.Start()

	return consumer, nil
}

// SubscribeTopics 注册消费者要监听的Topic列表
// 必须在第一次调用Poll之前调用, 同时触发消费者加入消费组并开始心跳
func (c *Consumer) SubscribeTopics(topics ...string) {
	c.actor.SubscribeTopics(topics...)
}

// Close 优雅关闭消费者，停止所有循环并最后提交一次偏移量
func (c *Consumer) Close() {
	c.actor.Close()
}

// IsReady 如果消费者处于 Ready 状态且有分配的分区，返回true
func (c *Consumer) IsReady() bool {
	return c.actor.IsReady()
}

// ID 返回消费者唯一ID
func (c *Consumer) ID() string {
	return c.actor.ID()
}

// State 返回消费者当前的状态
func (c *Consumer) State() ConsumerState {
	return c.actor.State()
}

// IsAutoCommitEnabled 返回是否启用了自动提交
func (c *Consumer) IsAutoCommitEnabled() bool {
	return c.config.EnableAutoCommit
}

// GetLastAutoCommitTime 返回最后一次自动提交的时间
// 注意: Actor 模型下不再单独跟踪此时间，返回当前时间
func (c *Consumer) GetLastAutoCommitTime() time.Time {
	return time.Now()
}

// Acknowledge 确认一批消息已经成功处理
// 这会将这批消息中最大的偏移量标记为准备提交
// 在自动提交模式下，后台循环会提交这些偏移量
// 在手动提交模式下，您仍需调用 CommitSync 来实际提交
func (c *Consumer) Acknowledge(messages ...ConsumerMessage) {
	c.actor.Acknowledge(messages...)
}

// CommitSync 同步提交所有当前分配分区的消费进度
// 这是一个阻塞操作，提交所有已拉取但尚未提交的消息ID
// ctx 用于控制超时，因为提交涉及数据库操作
func (c *Consumer) CommitSync(ctx context.Context) error {
	return c.actor.CommitSync(ctx)
}

// CommitMessage 提交单个消息ACK
// ctx 用于控制超时
func (c *Consumer) CommitMessage(ctx context.Context, msg ConsumerMessage) error {
	c.logger().Debug(fmt.Sprintf("🔍 [CommitMessage] Topic: %s, Partition: %d, ID: %d",
		msg.Topic, msg.Partition, msg.ID))

	// 先 Acknowledge，然后 CommitSync
	c.actor.Acknowledge(msg)
	return c.actor.CommitSync(ctx)
}

func (c *Consumer) logger() *slog.Logger {
	return logger.GetLogger().With("component", "consumer")
}
