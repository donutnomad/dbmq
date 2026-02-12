package dbmqapi

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq"
)

// DashboardAPI 仪表板 API
// @TAG(Dashboard)
// @PREFIX(/dbmq/api/v1)
type DashboardAPI interface {
	// GetDashboardData 获取仪表板数据
	// @GET(/dashboard/data)
	GetDashboardData(ctx context.Context) (DashboardDataResp, error)
}

type dashboardAPI struct {
	deps *Deps
}

func NewDashboardAPI(deps *Deps) DashboardAPI {
	return &dashboardAPI{deps: deps}
}

func (a *dashboardAPI) GetDashboardData(ctx context.Context) (DashboardDataResp, error) {
	topics, err := a.deps.MetricsClient.GetAllTopicsMetrics(ctx)
	topicResps := make([]TopicResp, 0)
	if err == nil {
		for _, t := range topics {
			topicResps = append(topicResps, TopicResp{
				Name:           t.TopicName,
				PartitionCount: uint(t.PartitionCount),
				MessageCount:   t.MessageCount,
				SizeBytes:      t.SizeBytes,
				CreatedAt:      t.CreatedAt.Format(time.RFC3339),
			})
		}
	}

	groups, err := a.deps.MetricsClient.GetAllConsumerGroupsMetrics(ctx)
	groupResps := make([]ConsumerGroupResp, 0)
	if err == nil {
		for _, g := range groups {
			partitionLags := make([]PartitionLagResp, len(g.PartitionLags))
			for j, lag := range g.PartitionLags {
				partitionLags[j] = PartitionLagResp{
					Topic:         lag.Topic,
					Partition:     lag.Partition,
					CurrentOffset: lag.CurrentOffset,
					LatestOffset:  lag.LatestOffset,
					Lag:           lag.Lag,
				}
			}
			groupResps = append(groupResps, ConsumerGroupResp{
				GroupID:       g.GroupID,
				State:         g.State,
				MemberCount:   len(g.Members),
				TotalLag:      g.Lag,
				PartitionLags: partitionLags,
			})
		}
	}

	var uptime float64
	if a.deps.StartTime != nil {
		uptime = float64(time.Now().Unix() - a.deps.StartTime())
	}

	return DashboardDataResp{
		Topics:         topicResps,
		ConsumerGroups: groupResps,
		System: SystemInfoResp{
			Uptime:  uptime,
			Version: dbmq.Version(),
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}, nil
}
