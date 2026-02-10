package dbmqapi

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"

	"github.com/gin-gonic/gin"
)

//go:embed static/*
var staticFS embed.FS

// ServerConfig 服务器配置
type ServerConfig struct {
	DB   interfaces.DB
	Port int
	Host string
}

// Server API 服务器
type Server struct {
	config    ServerConfig
	deps      *Deps
	engine    *gin.Engine
	server    *http.Server
	startTime time.Time
}

// NewServer 创建服务器
func NewServer(config ServerConfig) (*Server, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	if config.Port == 0 {
		config.Port = 8080
	}
	if config.Host == "" {
		config.Host = "localhost"
	}

	metricsClient, err := NewMetricsClient(config.DB)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics client: %w", err)
	}

	adminClient := dbmq.NewAdminClient(config.DB)

	s := &Server{
		config: config,
	}

	deps := &Deps{
		DB:                   config.DB,
		TopicQuery:           query.NewTopicQuery(config.DB),
		ConsumerQuery:        query.NewConsumerQuery(config.DB),
		MessageQuery:         query.NewMessageQuery(config.DB),
		MetricsClient:        metricsClient,
		AdminClient:          adminClient,
		ManualAssignmentRepo: manualassignmentrepo.New(config.DB),
		StartTime:            func() int64 { return s.startTime.Unix() },
	}

	s.deps = deps

	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(corsMiddleware())
	engine.Use(loggingMiddleware())

	s.engine = engine

	return s, nil
}

// RegisterAPIs 注册所有 API（由 gogen 生成的代码调用）
func (s *Server) RegisterAPIs() {
	// 静态文件
	staticFiles, err := fs.Sub(staticFS, "static")
	if err == nil {
		s.engine.StaticFS("/static", http.FS(staticFiles))
	}

	// 注册各个 API
	NewHealthAPIWrap(NewHealthAPI(s.deps), nil).BindAll(s.engine)
	NewDashboardAPIWrap(NewDashboardAPI(s.deps), nil).BindAll(s.engine)
	NewTopicAPIWrap(NewTopicAPI(s.deps), nil).BindAll(s.engine)
	NewConsumerGroupAPIWrap(NewConsumerGroupAPI(s.deps), nil).BindAll(s.engine)
	NewDBMQAPIWrap(NewDBMQAPI(s.deps), nil).BindAll(s.engine)
	NewClusterAPIWrap(NewClusterAPI(s.deps), nil).BindAll(s.engine)
	NewManualAssignmentAPIWrap(NewManualAssignmentAPI(s.deps), nil).BindAll(s.engine)
	// TopicProxyAPI 与 TopicAPI 有路由冲突，暂不注册
	// NewTopicProxyAPIWrap(NewTopicProxyAPI(s.deps), nil).BindAll(s.engine)
}

// Start 启动服务器
func (s *Server) Start() error {
	s.RegisterAPIs()

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.engine,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.startTime = time.Now()
	fmt.Printf("Starting DBMQ API server on %s\n", addr)
	return s.server.ListenAndServe()
}

// Stop 停止服务器
func (s *Server) Stop(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Engine 返回 gin 引擎（用于测试或自定义路由）
func (s *Server) Engine() *gin.Engine {
	return s.engine
}

// Deps 返回依赖（用于测试）
func (s *Server) Deps() *Deps {
	return s.deps
}

// corsMiddleware CORS 中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusOK)
			return
		}

		c.Next()
	}
}

// loggingMiddleware 日志中间件
func loggingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		duration := time.Since(start)
		fmt.Printf("[%s] %s %s %d %v\n",
			start.Format("2006-01-02 15:04:05"),
			c.Request.Method,
			c.Request.URL.Path,
			c.Writer.Status(),
			duration)
	}
}
