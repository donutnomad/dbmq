package dbmqapi

import (
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
)

// Deps API 依赖
type Deps struct {
	DB                   interfaces.DB
	TopicQuery           query.TopicQuery
	ConsumerQuery        query.ConsumerQuery
	MessageQuery         query.MessageQuery
	MetricsClient        *MetricsClient
	AdminClient          *dbmq.AdminClient
	ManualAssignmentRepo manualassignment.Repo
	StartTime            func() int64 // 返回启动时间戳（秒）
}
