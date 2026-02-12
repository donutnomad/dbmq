package dbmqapi

import (
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
)

type Deps struct {
	DB                   interfaces.DB
	TopicQuery           query.TopicQuery
	ConsumerQuery        query.ConsumerQuery
	MessageQuery         query.MessageQuery
	MetricsClient        *MetricsClient
	AdminClient          *dbmq.AdminClient
	Producer             *dbmq.Producer // 用于重发消息
	ManualAssignmentRepo manualassignment.Repo
	StartTime            func() int64 // 返回启动时间戳（秒）
}
