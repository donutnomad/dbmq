package dbmqapi

import (
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

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
