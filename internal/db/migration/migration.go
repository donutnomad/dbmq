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

// ApplySchemas 应用所有数据库表结构
func ApplySchemas(db interfaces.DB) error {
	return db.AutoMigrate(
		topicrepo.TopicPO{},
		messagerepo.MessagePO{},
		consumergrouprepo.GenerationPO{},
		heartbeatrepo.HeartbeatPO{},
		consumerprogressrepo.ProgressPO{},
		manualassignmentrepo.AssignmentPO{},
	)
}
