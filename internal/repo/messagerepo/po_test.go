package messagerepo

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMessagePOHasDashboardIndexes(t *testing.T) {
	messageType := reflect.TypeOf(MessagePO{})

	createdAtTag := messageType.Field(0).Tag.Get("gorm")
	idTag := messageType.Field(1).Tag.Get("gorm")
	topicTag := messageType.Field(2).Tag.Get("gorm")
	partitionTag := messageType.Field(3).Tag.Get("gorm")

	require.True(t, strings.Contains(createdAtTag, "idx_topic_created_id"))
	require.True(t, strings.Contains(createdAtTag, "idx_topic_partition_created_id"))
	require.True(t, strings.Contains(idTag, "idx_topic_created_id"))
	require.True(t, strings.Contains(idTag, "idx_topic_partition_created_id"))
	require.True(t, strings.Contains(topicTag, "idx_topic_created_id"))
	require.True(t, strings.Contains(topicTag, "idx_topic_partition_created_id"))
	require.True(t, strings.Contains(partitionTag, "idx_topic_partition_created_id"))
}
