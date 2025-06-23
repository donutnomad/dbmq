package pkg

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/donutnomad/dbmq/internal/db"

	"github.com/redis/go-redis/v9"
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
