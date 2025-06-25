package dbmq

import (
	"context"
	"database/sql"
	"github.com/donutnomad/dbmq/types"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminClient_CreateTopic(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试创建基本Topic
	req := NewTopicRequest{
		Name:          "test-topic-basic",
		NumPartitions: 3,
	}

	err := admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 验证Topic已创建
	var topic types.Topic
	err = dbClient.Where("topic_name = ?", req.Name).First(&topic).Error
	require.NoError(t, err)
	assert.Equal(t, req.Name, topic.TopicName)
	assert.Equal(t, uint(req.NumPartitions), topic.PartitionCount)
}

func TestAdminClient_CreateTopicWithConfig(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试创建带配置的Topic
	retentionHours := 24
	req := NewTopicRequest{
		Name:          "test-topic-config",
		NumPartitions: 2,
		Config: &TopicConfig{
			RetentionHours: &retentionHours,
			CleanupPolicy:  "delete",
		},
	}

	err := admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 验证Topic和配置已创建
	var topic types.Topic
	err = dbClient.Where("topic_name = ?", req.Name).First(&topic).Error
	require.NoError(t, err)
	assert.Equal(t, req.Name, topic.TopicName)
	assert.Equal(t, uint(req.NumPartitions), topic.PartitionCount)
	assert.True(t, topic.Configs.Valid)
	assert.Contains(t, topic.Configs.String, "retention.hours")
}

func TestAdminClient_CreateTopics_Batch(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试批量创建Topic
	requests := []NewTopicRequest{
		{
			Name:          "batch-topic-1",
			NumPartitions: 1,
		},
		{
			Name:          "batch-topic-2",
			NumPartitions: 2,
		},
		{
			Name:          "batch-topic-3",
			NumPartitions: 3,
		},
	}

	result := admin.CreateTopics(context.Background(), requests)
	require.Len(t, result.Results, 3)

	// 验证所有Topic都创建成功
	for i, topicResult := range result.Results {
		assert.NoError(t, topicResult.Error)
		assert.Equal(t, requests[i].Name, topicResult.Name)

		// 验证数据库中的Topic
		var topic types.Topic
		err := dbClient.Where("topic_name = ?", requests[i].Name).First(&topic).Error
		require.NoError(t, err)
		assert.Equal(t, uint(requests[i].NumPartitions), topic.PartitionCount)
	}
}

func TestAdminClient_CreateTopic_ValidationErrors(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试空名称
	req := NewTopicRequest{
		Name:          "",
		NumPartitions: 1,
	}
	err := admin.CreateTopic(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic name cannot be empty")

	// 测试无效分区数量
	req = NewTopicRequest{
		Name:          "test-topic",
		NumPartitions: 0,
	}
	err = admin.CreateTopic(context.Background(), req)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "number of partitions must be positive")

	// 先创建一个Topic
	req = NewTopicRequest{
		Name:          "existing-topic",
		NumPartitions: 1,
	}
	err = admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 测试重复创建
	err = admin.CreateTopic(context.Background(), req)
	assert.Error(t, err)
	var topicExistsErr *ErrTopicAlreadyExists
	assert.ErrorAs(t, err, &topicExistsErr)
	assert.Equal(t, req.Name, topicExistsErr.TopicName)
}

func TestAdminClient_ListTopics(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 创建几个Topic
	topics := []string{"list-topic-1", "list-topic-2", "list-topic-3"}
	for _, topicName := range topics {
		req := NewTopicRequest{
			Name:          topicName,
			NumPartitions: 1,
		}
		err := admin.CreateTopic(context.Background(), req)
		require.NoError(t, err)
	}

	// 列出所有Topic
	listedTopics, err := admin.ListTopics(context.Background())
	require.NoError(t, err)

	// 验证我们创建的Topic都在列表中
	for _, topicName := range topics {
		assert.Contains(t, listedTopics, topicName)
	}
}

func TestAdminClient_DescribeTopics(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 创建测试Topic
	retentionHours := 48
	req := NewTopicRequest{
		Name:          "describe-topic",
		NumPartitions: 5,
		Config: &TopicConfig{
			RetentionHours: &retentionHours,
			CleanupPolicy:  "delete",
		},
	}
	err := admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 描述Topic
	descriptions, err := admin.DescribeTopics(context.Background(), []string{req.Name})
	require.NoError(t, err)
	require.Len(t, descriptions, 1)

	desc := descriptions[req.Name]
	require.NotNil(t, desc)
	assert.Equal(t, req.Name, desc.Name)
	assert.Equal(t, req.NumPartitions, desc.NumPartitions)
	assert.NotNil(t, desc.Config)
	assert.Equal(t, &retentionHours, desc.Config.RetentionHours)
	assert.Equal(t, "delete", desc.Config.CleanupPolicy)
	assert.WithinDuration(t, time.Now(), desc.CreatedAt, 5*time.Second)
}

func TestAdminClient_DescribeTopics_NotFound(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 尝试描述不存在的Topic
	_, err := admin.DescribeTopics(context.Background(), []string{"non-existent-topic"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-existent-topic")
}

func TestAdminClient_DeleteTopics(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 创建测试Topic
	req := NewTopicRequest{
		Name:          "delete-topic",
		NumPartitions: 2,
	}
	err := admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 创建一些测试数据

	// 添加一些消息
	msg := &types.Message{
		Topic:     req.Name,
		Partition: 0,
		Body:      []byte("test message"),
		MessageKey: sql.NullString{
			String: "test-key",
			Valid:  true,
		},
	}
	err = dbClient.Create(msg).Error
	require.NoError(t, err)

	// 添加消费组偏移量
	offset := &types.ConsumerGroupOffset{
		GroupID:         "test-group",
		Topic:           req.Name,
		Partition:       0,
		CommittedOffset: 1,
		GenerationID:    1,
	}
	err = dbClient.Create(offset).Error
	require.NoError(t, err)

	// 删除Topic
	err = admin.DeleteTopics(context.Background(), []string{req.Name})
	require.NoError(t, err)

	// 验证Topic已删除
	var count int64
	err = dbClient.Model(&types.Topic{}).Where("topic_name = ?", req.Name).Count(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// 验证相关消息已删除
	err = dbClient.Model(&types.Message{}).Where("topic = ?", req.Name).Count(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// 验证相关偏移量已删除
	err = dbClient.Model(&types.ConsumerGroupOffset{}).Where("topic = ?", req.Name).Count(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestAdminClient_DeleteTopics_NotFound(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 尝试删除不存在的Topic
	err := admin.DeleteTopics(context.Background(), []string{"non-existent-topic"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "non-existent-topic")
}

func TestAdminClient_ValidateOnly(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试仅验证模式
	req := NewTopicRequest{
		Name:          "validate-only-topic",
		NumPartitions: 1,
		ValidateOnly:  true,
	}

	result := admin.CreateTopics(context.Background(), []NewTopicRequest{req})
	require.Len(t, result.Results, 1)
	assert.NoError(t, result.Results[0].Error)

	// 验证Topic没有实际创建
	var count int64
	err := dbClient.Model(&types.Topic{}).Where("topic_name = ?", req.Name).Count(&count).Error
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestAdminClient_ErrorTypes(t *testing.T) {
	dbClient, _ := setupIntegrationTest(t)

	admin := NewAdminClient(dbClient)

	// 测试ErrTopicAlreadyExists错误类型
	req := NewTopicRequest{
		Name:          "error-test-topic",
		NumPartitions: 1,
	}

	// 第一次创建应该成功
	err := admin.CreateTopic(context.Background(), req)
	require.NoError(t, err)

	// 第二次创建应该返回ErrTopicAlreadyExists
	err = admin.CreateTopic(context.Background(), req)
	require.Error(t, err)

	// 验证错误类型
	var topicExistsErr *ErrTopicAlreadyExists
	assert.ErrorAs(t, err, &topicExistsErr)
	assert.Equal(t, req.Name, topicExistsErr.TopicName)

	// 测试批量操作中的错误类型
	batchReqs := []NewTopicRequest{
		{Name: "new-topic-1", NumPartitions: 1},
		{Name: "error-test-topic", NumPartitions: 1}, // 这个会失败
		{Name: "new-topic-2", NumPartitions: 1},
	}

	result := admin.CreateTopics(context.Background(), batchReqs)
	require.Len(t, result.Results, 3)

	// 第一个应该成功
	assert.NoError(t, result.Results[0].Error)
	assert.Equal(t, "new-topic-1", result.Results[0].Name)

	// 第二个应该失败，且是ErrTopicAlreadyExists类型
	assert.Error(t, result.Results[1].Error)
	assert.ErrorAs(t, result.Results[1].Error, &topicExistsErr)
	assert.Equal(t, "error-test-topic", topicExistsErr.TopicName)

	// 第三个应该成功
	assert.NoError(t, result.Results[2].Error)
	assert.Equal(t, "new-topic-2", result.Results[2].Name)
}
