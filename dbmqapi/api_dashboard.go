package dbmqapi

import (
	"context"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/query"
	"golang.org/x/sync/errgroup"
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
	var (
		topics []query.TopicMetrics
		groups []query.ConsumerGroupMetrics
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		topics, err = a.deps.TopicQuery.GetAllTopicsMetrics(gCtx)
		return err
	})
	g.Go(func() error {
		var err error
		groups, err = a.deps.ConsumerQuery.GetAllConsumerGroupsMetrics(gCtx)
		return err
	})
	if err := g.Wait(); err != nil {
		return DashboardDataResp{}, err
	}

	topicResps := make([]TopicResp, 0, len(topics))
	for _, t := range topics {
		topicResps = append(topicResps, TopicResp{
			Name:           t.TopicName,
			PartitionCount: uint(t.PartitionCount),
			MessageCount:   t.MessageCount,
			SizeBytes:      t.SizeBytes,
			CreatedAt:      t.CreatedAt.Format(time.RFC3339),
		})
	}

	groupResps := make([]ConsumerGroupResp, 0, len(groups))
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
