package query

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllTopicPartitionStatsSQLQuotesPartition(t *testing.T) {
	require.True(t, strings.Contains(allTopicPartitionStatsSelectSQL, "`partition`"))
	require.False(t, strings.Contains(allTopicPartitionStatsSelectSQL, "\n\tpartition,"))
}
