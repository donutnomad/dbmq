package dbmq

import (
	"context"
	"time"

	leader "github.com/donutnomad/dbleader"
	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/domain/message"
	"github.com/donutnomad/dbmq/internal/domain/topic"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
)

const leaderLockName = "dbmq:coordinator"

type CoordinatorConfig struct {
	LockTable              string        // dbleader所在的表名（必填）
	NodeAddr               string        // 节点地址标识，用于 leader 选举（必填）
	DB                     interfaces.DB // 数据库连接
	HeartbeatTimeout       time.Duration // 消费者心跳超时时间，超过此时间认为消费者已死亡
	RebalanceInterval      time.Duration // 重新均衡检查间隔
	RebalanceTimeout       time.Duration // 重新均衡操作的上下文超时时间
	RetentionCheckInterval time.Duration // 消息保留清理检查间隔
	DefaultRetentionAge    time.Duration // 没有特定保留策略的Topic的默认保留时间
	HeartbeatRetentionAge  time.Duration // 消费者心跳记录保留时长,超过此时长的心跳记录会被清理
}

func (cfg *CoordinatorConfig) validate() {
	if cfg.RebalanceInterval == 0 { // 重平衡间隔
		cfg.RebalanceInterval = 10 * time.Second
	}
	if cfg.HeartbeatTimeout == 0 { // 消费心跳时间
		cfg.HeartbeatTimeout = 30 * time.Second
	}
	if cfg.DefaultRetentionAge == 0 { // 消息保留: 默认7天保留期
		cfg.DefaultRetentionAge = 7 * 24 * time.Hour
	}
	if cfg.RetentionCheckInterval == 0 { // 消息保留: 默认每小时检查一次
		cfg.RetentionCheckInterval = 1 * time.Hour
	}
	if cfg.RebalanceTimeout <= 0 {
		cfg.RebalanceTimeout = 15 * time.Second
	}
	if cfg.HeartbeatRetentionAge == 0 { // 心跳记录保留: 默认7天
		cfg.HeartbeatRetentionAge = 7 * 24 * time.Hour
	}
}

// Coordinator 处理重平衡和消息清理
type Coordinator struct {
	repos
	config  CoordinatorConfig // 配置
	manager *leader.Manager   // dbleader 管理器，负责 leader 选举和续约
}

// NewCoordinator 创建一个新的协调器 NodeAddr 是必填参数
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	if config.LockTable == "" {
		panic("CoordinatorConfig.LockTable is required for leader election")
	}
	if config.NodeAddr == "" {
		panic("CoordinatorConfig.NodeAddr is required for leader election")
	}
	config.validate()

	repos := newRepos(config.DB)
	return &Coordinator{
		config: config,
		repos:  repos,
		manager: leader.NewManager(leader.NewMysqlLockStore(config.DB, config.LockTable), leaderLockName, config.NodeAddr, []leader.LeaderTask{
			newCoordinatorTask(&config, repos),
			newCleanerTask(&config, repos),
		}),
	}
}

func (c *Coordinator) Start() {
	c.manager.Start()
}

func (c *Coordinator) Stop() {
	c.manager.Stop()
}

func (c *Coordinator) IsLeader() bool {
	return c.manager.IsLeader()
}

// CleanupExpiredMessages 删除过期的消息
func (c *Coordinator) CleanupExpiredMessages(ctx context.Context) error {
	newCleanerTask(&c.config, c.repos).clean(ctx)
	return nil
}

type repos struct {
	topicRepo            topic.Repo
	messageRepo          message.Repo
	heartbeatRepo        heartbeat.Repo
	groupRepo            consumergroup.Repo
	progressRepo         consumerprogress.Repo
	manualAssignmentRepo manualassignment.Repo
}

func newRepos(db interfaces.DB) repos {
	return repos{
		topicRepo:            topicrepo.New(db),
		messageRepo:          messagerepo.New(db),
		heartbeatRepo:        heartbeatrepo.New(db),
		groupRepo:            consumergrouprepo.New(db),
		progressRepo:         consumerprogressrepo.New(db),
		manualAssignmentRepo: manualassignmentrepo.New(db),
	}
}
