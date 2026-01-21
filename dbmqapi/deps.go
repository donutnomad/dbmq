package dbmqapi

import (
	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/repo"
)

// Deps API 依赖
type Deps struct {
	DB            interfaces.DB
	MetricsClient *MetricsClient
	AdminClient   *dbmq.AdminClient
	Repo          *repo.MqRepo
	StartTime     func() int64 // 返回启动时间戳（秒）
}
