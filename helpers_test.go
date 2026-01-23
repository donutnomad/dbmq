package dbmq

import (
	"context"
	"fmt"
	"github.com/donutnomad/dbmq/internal/db/migration"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"gorm.io/driver/mysql"
	"gorm.io/gorm/logger"
	"log"
	"testing"
	"time"

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
func setupIntegrationTest(t *testing.T) (interfaces.DB, redis.UniversalClient) {
	// Create a unique DB name for this test run to ensure isolation
	dbName := fmt.Sprintf("dbmq_test_%d", time.Now().UnixNano())

	mysqlConf := MySQLConfig{
		Host:     testMySQLHost,
		Port:     testMySQLPort,
		User:     testMySQLUser,
		Password: testMySQLPassword,
		DBName:   dbName,
	}

	// Create the database
	err := CreateDatabaseIfNotExists(mysqlConf)
	require.NoError(t, err, "Failed to create test database")

	// Connect to the new database
	dbClient, err := InitMySQL(mysqlConf)
	require.NoError(t, err, "Failed to connect to test database")

	// Apply schemas
	err = DropAllTables(dbClient)
	require.NoError(t, err, "Failed to drop old tables")
	err = migration.ApplySchemas(dbClient)
	require.NoError(t, err, "Failed to apply schemas to test database")

	// Connect to Redis
	redisConf := RedisConfig{Host: testRedisHost, Port: testRedisPort, DB: testRedisDB}
	redisClient, err := InitRedis(redisConf)
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
		rootConf := MySQLConfig{
			Host: testMySQLHost, Port: testMySQLPort, User: testMySQLUser, Password: testMySQLPassword,
		}
		rootDB, _ := InitMySQL(rootConf)
		rootDB.Exec(fmt.Sprintf("DROP DATABASE %s", dbName))
		rootSQLDB, _ := rootDB.DB()
		rootSQLDB.Close()
	})

	return dbClient, redisClient
}

// DropAllTables 删除所有mq_开头的表
func DropAllTables(db interfaces.DB) error {
	// 获取所有以 mq_ 开头的表名
	var tableNames []string
	if err := db.Raw("SHOW TABLES LIKE 'mq_%%'").Scan(&tableNames).Error; err != nil {
		return fmt.Errorf("failed to list mq_ tables: %w", err)
	}

	// 禁用外键检查，以便可以删除有依赖的表
	if err := db.Exec("SET FOREIGN_KEY_CHECKS = 0").Error; err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %w", err)
	}

	// 遍历并删除每个表
	for _, tableName := range tableNames {
		if err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)).Error; err != nil {
			// 重新启用外键检查，并返回错误
			_ = db.Exec("SET FOREIGN_KEY_CHECKS = 1").Error
			return fmt.Errorf("failed to drop table %s: %w", tableName, err)
		}
	}

	// 重新启用外键检查
	if err := db.Exec("SET FOREIGN_KEY_CHECKS = 1").Error; err != nil {
		return fmt.Errorf("failed to enable foreign key checks: %w", err)
	}

	return nil
}

// MySQLConfig holds the configuration for the MySQL connection.
type MySQLConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// RedisConfig holds the configuration for the Redis connection.
type RedisConfig struct {
	Host string
	Port int
	DB   int
}

// InitMySQL initializes the database connection for MySQL.
func InitMySQL(mysqlConf MySQLConfig) (interfaces.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		mysqlConf.User, mysqlConf.Password, mysqlConf.Host, mysqlConf.Port, mysqlConf.DBName)

	dbClient, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	sqlDB, err := dbClient.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}
	sqlDB.SetMaxIdleConns(10)
	// 连接池调优
	sqlDB.SetMaxOpenConns(200) // 增加连接数
	sqlDB.SetConnMaxLifetime(time.Hour)

	return dbClient, nil
}

// CreateDatabaseIfNotExists 创建数据库（如果不存在）
// 此函数打开临时连接到MySQL服务器来执行此操作
// 设计为在启动时调用一次
func CreateDatabaseIfNotExists(config MySQLConfig) error {
	// 不包含数据库名的DSN，用于连接到MySQL服务器
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=True&loc=Local",
		config.User, config.Password, config.Host, config.Port)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent), // 静默模式，避免日志干扰
	})
	if err != nil {
		return fmt.Errorf("failed to connect to mysql server for db creation: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql.DB for db creation: %w", err)
	}
	defer func() {
		_ = sqlDB.Close()
	}()

	// 创建数据库，使用UTF8MB4字符集和Unicode排序规则
	exec := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", config.DBName)
	if err := db.Exec(exec).Error; err != nil {
		return fmt.Errorf("failed to create database %s: %w", config.DBName, err)
	}

	return nil
}

// InitRedis initializes the database connection for Redis.
func InitRedis(redisConf RedisConfig) (redis.UniversalClient, error) {
	redisClient := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%d", redisConf.Host, redisConf.Port),
		DB:           redisConf.DB,
		ReadTimeout:  30 * time.Second, // 增加读超时，避免PubSub连接频繁断开
		WriteTimeout: 10 * time.Second, // 增加写超时
		PoolTimeout:  30 * time.Second, // 连接池超时
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %w", err)
	}

	return redisClient, nil
}
