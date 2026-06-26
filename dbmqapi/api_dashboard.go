package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/domain/manualassignment"
	"github.com/donutnomad/dbmq/internal/query"
	"golang.org/x/sync/errgroup"
)

// DashboardAPI 仪表板 API
// @TAG(Dashboard)
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

func buildManualAssignmentResponses(assignments []*manualassignment.Assignment) []ManualAssignmentResp {
	result := make([]ManualAssignmentResp, len(assignments))
	for i, assign := range assignments {
		result[i] = ManualAssignmentResp{
			ID:                assign.ID,
			GroupID:           assign.GroupID,
			ConsumerIDPattern: assign.ConsumerIDPattern,
			Topic:             assign.Topic,
			Partition:         assign.Partition,
			CreatedAt:         assign.CreatedAt,
			UpdatedAt:         assign.UpdatedAt,
		}
	}
	return result
}

func buildStatsResp(topics []query.TopicMetrics, groups []query.ConsumerGroupMetrics, messageTableStats *query.TableStats, uptime int64) DBMQStatsResp {
	var partitionCount int
	for _, topic := range topics {
		partitionCount += topic.PartitionCount
	}

	var totalMessages int64
	var totalSizeBytes int64
	if messageTableStats != nil {
		totalMessages = messageTableStats.EstimatedRows
		totalSizeBytes = messageTableStats.TotalBytes
	}

	return DBMQStatsResp{
		Cluster: ClusterMetricsResp{
			TopicCount:         len(topics),
			PartitionCount:     partitionCount,
			ConsumerGroupCount: len(groups),
			TotalMessages:      totalMessages,
			TotalSizeBytes:     totalSizeBytes,
		},
		Broker: BrokerResp{
			BrokerID: 0,
			Host:     "localhost",
			Port:     9092,
			Version:  dbmq.Version(),
			Uptime:   fmt.Sprintf("%ds", uptime),
		},
		System: SystemInfoResp{
			Uptime:  float64(uptime),
			Version: dbmq.Version(),
		},
	}
}

func buildDashboardData(topics []query.TopicMetrics, groups []query.ConsumerGroupMetrics, assignments []*manualassignment.Assignment, messageTableStats *query.TableStats, uptime int64) DashboardDataResp {
	var (
		topicResps = make([]TopicResp, 0, len(topics))
		groupResps = make([]ConsumerGroupResp, 0, len(groups))
	)

	for _, t := range topics {
		topicResps = append(topicResps, TopicResp{
			Name:           t.TopicName,
			PartitionCount: uint(t.PartitionCount),
			MessageCount:   t.MessageCount,
			SizeBytes:      t.SizeBytes,
			CreatedAt:      t.CreatedAt.Format(time.RFC3339),
		})
	}

	for _, g := range groups {
		groupResps = append(groupResps, ConsumerGroupResp{
			GroupID:     g.GroupID,
			State:       g.State,
			MemberCount: len(g.Members),
			TotalLag:    g.Lag,
		})
	}

	return DashboardDataResp{
		Topics:            topicResps,
		ConsumerGroups:    groupResps,
		Stats:             buildStatsResp(topics, groups, messageTableStats, uptime),
		ManualAssignments: buildManualAssignmentResponses(assignments),
		System: SystemInfoResp{
			Uptime:  float64(uptime),
			Version: dbmq.Version(),
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

func (a *dashboardAPI) GetDashboardData(ctx context.Context) (DashboardDataResp, error) {
	var (
		topics            []query.TopicMetrics
		groups            []query.ConsumerGroupMetrics
		assignments       []*manualassignment.Assignment
		messageTableStats *query.TableStats
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		topics, err = a.deps.TopicQuery.GetAllTopicsMetrics(gCtx)
		return err
	})
	g.Go(func() error {
		var err error
		groups, err = a.deps.ConsumerQuery.GetAllConsumerGroupsSummary(gCtx)
		return err
	})
	g.Go(func() error {
		var err error
		assignments, err = a.deps.ManualAssignmentRepo.GetAll(gCtx)
		return err
	})
	g.Go(func() error {
		var err error
		messageTableStats, err = a.deps.ClusterQuery.GetMessageTableStats(gCtx)
		return err
	})
	if err := g.Wait(); err != nil {
		return DashboardDataResp{}, err
	}

	var uptime int64
	if a.deps.StartTime != nil {
		uptime = time.Now().Unix() - a.deps.StartTime()
	}

	return buildDashboardData(topics, groups, assignments, messageTableStats, uptime), nil
}
