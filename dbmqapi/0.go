package dbmqapi

import (
	"context"
	"net/http"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/domain/consumergroup"
	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/domain/heartbeat"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
	"github.com/gin-gonic/gin"
)

//go:generate go tool gogen ./...

type Deps struct {
	DB            interfaces.DB
	TopicQuery    query.TopicQuery
	ConsumerQuery query.ConsumerQuery
	MessageQuery  query.MessageQuery
	ClusterQuery  query.ClusterQuery

	AdminClient          *dbmq.AdminClient
	Producer             *dbmq.Producer // 用于重发消息
	ManualAssignmentRepo manualassignment.Repo
	ConsumerGroupRepo    consumergroup.Repo
	HeartbeatRepo        heartbeat.Repo
	ProgressRepo         consumerprogress.Repo
	StartTime            func() int64 // 返回启动时间戳（秒）
}

func (d *Deps) GetBrokerMetrics(ctx context.Context) (*query.BrokerMetrics, error) {
	result, err := d.ClusterQuery.GetBrokerMetrics(ctx, d.StartTime)
	if err != nil {
		return nil, err
	}
	result.Version = dbmq.Version()
	return result, nil
}

// onGinBind 绑定请求参数
func onGinBind(c *gin.Context, val any, typ string) bool {
	var err error
	switch typ {
	case "JSON":
		err = c.ShouldBindJSON(val)
	case "FORM":
		err = c.ShouldBind(val)
	case "QUERY":
		err = c.ShouldBindQuery(val)
	default:
		err = c.ShouldBind(val)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return false
	}
	return true
}

// onGinResponse 响应处理
func onGinResponse[T any](c *gin.Context, data T, err error) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true, "data": data})
}

// onGinBindErr 绑定错误处理
func onGinBindErr(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}
