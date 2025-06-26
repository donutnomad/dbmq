package db

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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
func InitMySQL(mysqlConf MySQLConfig) (*gorm.DB, error) {
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

// InitRedis initializes the database connection for Redis.
func InitRedis(redisConf RedisConfig) (*redis.Client, error) {
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
