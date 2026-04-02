package migration

import (
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"
	"github.com/donutnomad/dbmq/internal/repo/messagerepo"
	"github.com/donutnomad/dbmq/internal/repo/topicrepo"
)

const createLeaderLockTable = `
CREATE TABLE IF NOT EXISTS mq_coordinator_leader_lock (
    lock_name   VARCHAR(64)     NOT NULL COMMENT '锁名称/组件名',
    leader_ip   VARCHAR(64)     NOT NULL COMMENT '当前持有锁的节点地址',
    expire_time DATETIME(3)     NOT NULL COMMENT '锁的绝对过期时间',
    version     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '每次易主 +1，Fencing Token',
    PRIMARY KEY (lock_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

// ApplySchemas 应用所有数据库表结构
func ApplySchemas(db interfaces.DB) error {
	if err := db.AutoMigrate(
		topicrepo.TopicPO{},
		messagerepo.MessagePO{},
		consumergrouprepo.GenerationPO{},
		heartbeatrepo.HeartbeatPO{},
		consumerprogressrepo.ProgressPO{},
		manualassignmentrepo.AssignmentPO{},
	); err != nil {
		return err
	}
	return db.Exec(createLeaderLockTable).Error
}
