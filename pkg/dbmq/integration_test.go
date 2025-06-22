//go:build integration

package dbmq

import (
	"context"
	"dbmq/internal/db"
	"dbmq/pkg/dbmq/types"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

const (
	// --- Test Database Credentials ---
	testMySQLHost     = "127.0.0.1"
	testMySQLPort     = 3306
	testMySQLUser     = "root"
	testMySQLPassword = "123456"

	testRedisHost = "127.0.0.1"
	testRedisPort = 6379
	testRedisDB   = 2
	// ---
)

// setupIntegrationTest prepares the environment for an integration test.
// It creates a unique database for the test run, applies schemas, and cleans up afterwards.
func setupIntegrationTest(t *testing.T) (*gorm.DB, *redis.Client) {
	// Create a unique DB name for this test run to ensure isolation
	dbName := fmt.Sprintf("dbmq_test_%d", time.Now().UnixNano())

	mysqlConf := db.MySQLConfig{
		Host:     testMySQLHost,
		Port:     testMySQLPort,
		User:     testMySQLUser,
		Password: testMySQLPassword,
		DBName:   dbName,
	}

	// Create the database
	err := db.CreateDatabaseIfNotExists(mysqlConf)
	require.NoError(t, err, "Failed to create test database")

	// Connect to the new database
	dbClient, err := db.InitMySQL(mysqlConf)
	require.NoError(t, err, "Failed to connect to test database")

	// Apply schemas
	err = db.ApplySchemas(dbClient)
	require.NoError(t, err, "Failed to apply schemas to test database")

	// Connect to Redis
	redisConf := db.RedisConfig{Host: testRedisHost, Port: testRedisPort, DB: testRedisDB}
	redisClient, err := db.InitRedis(redisConf)
	require.NoError(t, err, "Failed to connect to redis")

	// Flush Redis DB to ensure clean state
	err = redisClient.FlushDB(context.Background()).Err()
	require.NoError(t, err)

	// Teardown function to be called at the end of the test
	t.Cleanup(func() {
		log.Printf("Tearing down test, dropping database: %s", dbName)
		sqlDB, _ := dbClient.DB()
		sqlDB.Close()
		redisClient.Close()

		// A separate connection to drop the database
		rootConf := db.MySQLConfig{
			Host: testMySQLHost, Port: testMySQLPort, User: testMySQLUser, Password: testMySQLPassword,
		}
		rootDB, _ := db.InitMySQL(rootConf)
		rootDB.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
		rootSQLDB, _ := rootDB.DB()
		rootSQLDB.Close()
	})

	return dbClient, redisClient
}

func TestIntegration_FullFlow(t *testing.T) {
	dbClient, redisClient := setupIntegrationTest(t)

	// 1. Create a Topic for the test
	testTopic := &types.Topic{
		TopicName:      "integration-topic",
		PartitionCount: 1,
	}
	err := dbClient.Create(testTopic).Error
	require.NoError(t, err)

	// 2. Start Coordinator
	coordConf := CoordinatorConfig{
		DB:                dbClient,
		HeartbeatTimeout:  5 * time.Second,
		RebalanceInterval: 1 * time.Second,
	}
	coordinator := NewCoordinator(coordConf)
	coordinator.Start()
	defer coordinator.Stop()

	// Wait for the coordinator to become leader
	require.Eventually(t, coordinator.IsLeader, 10*time.Second, 100*time.Millisecond, "Coordinator did not become leader")

	// 3. Start Consumer
	consumerGroup := "test-group-1"
	consumerConf := ConsumerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		GroupID:             consumerGroup,
		NotificationEnabled: true,
		Topics:              []string{testTopic.TopicName},
		HeartbeatInterval:   1 * time.Second,
	}
	consumer, err := NewConsumer(consumerConf)
	require.NoError(t, err)
	err = consumer.Subscribe(testTopic.TopicName)
	require.NoError(t, err)
	defer consumer.Close()

	// Wait for the rebalance to happen and partitions to be assigned
	time.Sleep(3 * time.Second) // Give coordinator time to run rebalance

	// 4. Start Producer and Send a Message
	producerConf := ProducerConfig{
		DB:                  dbClient,
		Redis:               redisClient,
		NotificationEnabled: true,
	}
	producer, err := NewProducer(producerConf)
	require.NoError(t, err)
	defer producer.Close()

	testKey := []byte("test-key")
	testValue := []byte("hello world")
	sentMsg := &ProducerMessage{
		Topic: testTopic.TopicName,
		Key:   testKey,
		Value: testValue,
	}
	sendResult, err := producer.Send(context.Background(), sentMsg)
	require.NoError(t, err)
	assert.Equal(t, uint(0), sendResult.Partition)

	// 5. Consumer Polls and Receives the Message
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	receivedMsgs, err := consumer.Poll(ctx, 5*time.Second)
	require.NoError(t, err)
	require.Len(t, receivedMsgs, 1, "Consumer should have received exactly one message")

	receivedMsg := receivedMsgs[0]
	assert.Equal(t, testTopic.TopicName, receivedMsg.Topic)
	assert.Equal(t, testKey, receivedMsg.Key)
	assert.Equal(t, testValue, receivedMsg.Value)
	assert.Equal(t, sendResult.Offset, receivedMsg.Offset)

	// 6. Commit the offset
	err = consumer.CommitSync()
	require.NoError(t, err)

	// 7. Verify the commit in the database
	var committedOffset types.ConsumerGroupOffset
	err = dbClient.Where(&types.ConsumerGroupOffset{
		GroupID:   consumerGroup,
		Topic:     testTopic.TopicName,
		Partition: 0,
	}).First(&committedOffset).Error
	require.NoError(t, err, "Failed to find the committed offset in the database")
	assert.Equal(t, sendResult.Offset, committedOffset.CommittedOffset, "Committed offset in DB does not match sent message offset")
	log.Printf("Successfully verified committed offset in DB: %d", committedOffset.CommittedOffset)
}
