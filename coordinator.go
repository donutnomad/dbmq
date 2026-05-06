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

const (
	leaderLockPrefix       = "mq_coordinator_leader_lock_"
	leaderLockTableDefault = "mq_coordinator_leader_lock"
	leaderLockNameDefault  = "dbmq:coordinator"
)

type CoordinatorConfig struct {
	LockSuffix             string        // 锁后缀，用于区分不同应用的协调器
	LockTable              string        // dbleader所在的表名，默认 mq_coordinator_leader_lock
	NodeAddr               string        // 节点地址标识，用于 leader 选举（必填）
	DB                     interfaces.DB // 数据库连接
	HeartbeatTimeout       time.Duration // 消费者心跳超时时间，超过此时间认为消费者已死亡
	RebalanceInterval      time.Duration // 重新均衡检查间隔
	RebalanceTimeout       time.Duration // 重新均衡操作的上下文超时时间
	RetentionCheckInterval time.Duration // 消息保留清理检查间隔
	DefaultRetentionAge    time.Duration // 没有特定保留策略的Topic的默认保留时间
}

// Coordinator 处理重平衡和消息清理
type Coordinator struct {
	repos
	config  CoordinatorConfig // 配置
	manager *leader.Manager   // dbleader 管理器，负责 leader 选举和续约
}

// groupSnapshot 缓存每次成功重新均衡后的成员订阅和分区元数据
type groupSnapshot struct {
	generationID         uint              // 最后一次成功 rebalance 后的 generation_id
	memberTopics         map[string]string // consumerID -> 订阅Topic哈希，用于检测订阅变更
	partitionHash        string            // 相关Topic及分区数量的哈希，用于检测Topic/分区变化
	manualAssignmentHash string            // 手动分配规则的哈希，用于检测手动分配变化
}

// NewCoordinator 创建一个新的协调器 NodeAddr 是必填参数
func NewCoordinator(config CoordinatorConfig) *Coordinator {
	if config.LockTable == "" {
		config.LockTable = leaderLockTableDefault
	}
	if config.NodeAddr == "" {
		panic("CoordinatorConfig.NodeAddr is required for leader election")
	}
	if config.RebalanceInterval == 0 { // 重平衡间隔
		config.RebalanceInterval = 10 * time.Second
	}
	if config.HeartbeatTimeout == 0 { // 消费心跳时间
		config.HeartbeatTimeout = 30 * time.Second
	}
	if config.DefaultRetentionAge == 0 { // 消息保留: 默认7天保留期
		config.DefaultRetentionAge = 7 * 24 * time.Hour
	}
	if config.RetentionCheckInterval == 0 { // 消息保留: 默认每小时检查一次
		config.RetentionCheckInterval = 1 * time.Hour
	}
	if config.RebalanceTimeout <= 0 {
		config.RebalanceTimeout = 15 * time.Second
	}

	repos := newRepos(config.DB)
	lockName := leaderLockNameDefault
	if config.LockSuffix != "" {
		lockName = leaderLockPrefix + config.LockSuffix
	}
	return &Coordinator{
		config: config,
		repos:  repos,
		manager: leader.NewManager(leader.NewMysqlLockStore(config.DB, config.LockTable), lockName, config.NodeAddr, []leader.LeaderTask{
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
