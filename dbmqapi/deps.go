package dbmqapi

import (
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/interfaces"
)

// Deps API 依赖
type Deps struct {
	DB                   interfaces.DB
	MetricsClient        *MetricsClient
	AdminClient          *dbmq.AdminClient
	ManualAssignmentRepo manualassignment.Repo
	StartTime            func() int64 // 返回启动时间戳（秒）
}
