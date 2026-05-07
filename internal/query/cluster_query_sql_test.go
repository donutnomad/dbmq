package query

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClusterSummarySQLUsesLightweightCounts(t *testing.T) {
	require.Contains(t, topicSummaryStatsSelectSQL, "COUNT(*) AS topic_count")
	require.Contains(t, topicSummaryStatsSelectSQL, "SUM(partition_count)")
	require.Contains(t, consumerGroupCountTable, "mq_consumer_group_generations")
}
