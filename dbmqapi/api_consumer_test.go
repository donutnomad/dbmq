package dbmqapi

import (
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/query"
	"github.com/stretchr/testify/require"
)

func TestBuildConsumerResponsesMapsConsumerMetrics(t *testing.T) {
	now := time.Date(2026, 5, 7, 10, 0, 0, 0, time.UTC)

	resp := buildConsumerResponses([]query.ConsumerMetrics{
		{
			GroupID:          "orders-group",
			ConsumerID:       "consumer-1",
			ClientID:         "consumer-1",
			Host:             "localhost",
			GenerationID:     2,
			Status:           "online",
			LastHeartbeat:    now,
			SubscribedTopics: []string{"orders"},
			Assignment: []query.PartitionInfo{
				{Topic: "orders", Partition: 0},
				{Topic: "orders", Partition: 1},
			},
		},
	})

	require.Len(t, resp, 1)
	require.Equal(t, "orders-group", resp[0].GroupID)
	require.Equal(t, "consumer-1", resp[0].MemberID)
	require.Equal(t, 2, resp[0].GenerationID)
	require.Equal(t, "online", resp[0].Status)
	require.Equal(t, []string{"orders"}, resp[0].SubscribedTopics)
	require.Equal(t, []int{0, 1}, resp[0].Assignment["orders"])
}
