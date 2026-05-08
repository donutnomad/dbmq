package dbmqapi

import (
	"context"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/domain/consumerprogress"
	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

// stubConsumerQueryForCleanup 仅实现 cleanup 测试需要的两个方法，其他方法 panic 防止误用。
type stubConsumerQueryForCleanup struct {
	query.ConsumerQuery
	stale    []query.StaleProgressDTO
	detached []query.DetachedProgressDTO
	staleErr error
	detErr   error
}

func (s *stubConsumerQueryForCleanup) GetStaleProgress(_ context.Context, _ uint) ([]query.StaleProgressDTO, error) {
	return s.stale, s.staleErr
}

func (s *stubConsumerQueryForCleanup) GetDetachedProgress(_ context.Context) ([]query.DetachedProgressDTO, error) {
	return s.detached, s.detErr
}

// stubProgressRepoForCleanup 记录 DeleteByGroupTopicPartition 是否被调用。
type stubProgressRepoForCleanup struct {
	consumerprogress.Repo
	called bool
}

func (s *stubProgressRepoForCleanup) DeleteByGroupTopicPartition(_ context.Context, _ string, _ string, _ uint) error {
	s.called = true
	return nil
}

func TestBuildPartitionLagResponsesUsesQueryMetrics(t *testing.T) {
	lags := []query.PartitionLagMetrics{
		{
			Topic:              "orders",
			Partition:          2,
			CurrentOffset:      42,
			LatestOffset:       77,
			Lag:                5,
			ConsumedMessages:   12,
			RemainingMessages:  5,
			ConsumedPercentage: 70.5,
		},
	}

	resp := buildPartitionLagResponses(lags)
	require.Len(t, resp, 1)
	require.Equal(t, int64(12), resp[0].ConsumedMessages)
	require.Equal(t, int64(5), resp[0].RemainingMessages)
	require.Equal(t, 70.5, resp[0].ConsumedPercentage)
	require.Equal(t, int64(42), resp[0].CurrentOffset)
	require.Equal(t, int64(77), resp[0].LatestOffset)
}

func TestBuildConsumerResponsesIncludesAssignmentsAndStatus(t *testing.T) {
	now := time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC)
	offlineAt := now.Add(-2 * time.Minute)

	consumers := []query.ConsumerMetrics{
		{
			GroupID:          "orders-group",
			ConsumerID:       "ubuntu:00:0c:29:3d:f7:5d:abc",
			ClientID:         "ubuntu:00:0c:29:3d:f7:5d:abc",
			Host:             "localhost",
			GenerationID:     12,
			Offline:          false,
			Status:           "online",
			LastHeartbeat:    now,
			SubscribedTopics: []string{"orders", "payments"},
			Assignment: []query.PartitionInfo{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
				{Topic: "payments", Partition: 2},
			},
		},
		{
			GroupID:          "billing-group",
			ConsumerID:       "consumer-offline",
			ClientID:         "consumer-offline",
			Host:             "localhost",
			GenerationID:     3,
			Offline:          true,
			Status:           "offline",
			LastHeartbeat:    now.Add(-time.Hour),
			OfflineAt:        &offlineAt,
			SubscribedTopics: []string{"billing"},
		},
	}

	resp := buildConsumerResponses(consumers)
	require.Len(t, resp, 2)
	require.Equal(t, "orders-group", resp[0].GroupID)
	require.Equal(t, "ubuntu:00:0c:29:3d:f7:5d:abc", resp[0].MemberID)
	require.Equal(t, []int{0, 1}, resp[0].Assignment["orders"])
	require.Equal(t, []int{2}, resp[0].Assignment["payments"])
	require.Equal(t, "online", resp[0].Status)
	require.Equal(t, []string{"orders", "payments"}, resp[0].SubscribedTopics)
	require.NotNil(t, resp[1].OfflineAt)
	require.Equal(t, "offline", resp[1].Status)
}

func TestDefaultStaleDaysMatchesDocumented(t *testing.T) {
	// 计划文档约定：默认 stale 判定阈值为 10 天。
	// 任何变更都应该在文档/前端 UI 同步更新。
	require.Equal(t, uint(10), defaultStaleDays)
}

func TestDeleteStaleProgressRequiresMatch(t *testing.T) {
	cq := &stubConsumerQueryForCleanup{stale: nil}
	repo := &stubProgressRepoForCleanup{}
	api := &consumerGroupAPI{deps: &Deps{ConsumerQuery: cq, ProgressRepo: repo}}

	_, err := api.DeleteStaleProgress(context.Background(), "g1", DeleteStaleProgressReq{
		Topic: "t1", Partition: 0, StaleDays: 10,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no longer stale")
	require.False(t, repo.called, "repo Delete must not be called when match check fails")
}

func TestDeleteStaleProgressDeletesOnMatch(t *testing.T) {
	cq := &stubConsumerQueryForCleanup{stale: []query.StaleProgressDTO{
		{GroupID: "g1", Topic: "t1", Partition: 0},
	}}
	repo := &stubProgressRepoForCleanup{}
	api := &consumerGroupAPI{deps: &Deps{ConsumerQuery: cq, ProgressRepo: repo}}

	resp, err := api.DeleteStaleProgress(context.Background(), "g1", DeleteStaleProgressReq{
		Topic: "t1", Partition: 0, StaleDays: 10,
	})
	require.NoError(t, err)
	require.True(t, repo.called)
	require.NotEmpty(t, resp.Message)
}

func TestDeleteDetachedProgressRequiresMatch(t *testing.T) {
	cq := &stubConsumerQueryForCleanup{detached: nil}
	repo := &stubProgressRepoForCleanup{}
	api := &consumerGroupAPI{deps: &Deps{ConsumerQuery: cq, ProgressRepo: repo}}

	_, err := api.DeleteDetachedProgress(context.Background(), "g1", DeleteDetachedProgressReq{
		Topic: "t1", Partition: 0,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not detached")
	require.False(t, repo.called, "repo Delete must not be called when match check fails")
}

func TestDeleteDetachedProgressDeletesOnMatch(t *testing.T) {
	cq := &stubConsumerQueryForCleanup{detached: []query.DetachedProgressDTO{
		{GroupID: "g1", Topic: "t1", Partition: 0},
	}}
	repo := &stubProgressRepoForCleanup{}
	api := &consumerGroupAPI{deps: &Deps{ConsumerQuery: cq, ProgressRepo: repo}}

	resp, err := api.DeleteDetachedProgress(context.Background(), "g1", DeleteDetachedProgressReq{
		Topic: "t1", Partition: 0,
	})
	require.NoError(t, err)
	require.True(t, repo.called)
	require.NotEmpty(t, resp.Message)
}
