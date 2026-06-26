package dbmqapi

import (
	"context"
	"embed"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/donutnomad/dbmq"
	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/donutnomad/dbmq/internal/query"
	"github.com/donutnomad/dbmq/internal/repo/consumergrouprepo"
	"github.com/donutnomad/dbmq/internal/repo/consumerprogressrepo"
	"github.com/donutnomad/dbmq/internal/repo/heartbeatrepo"
	"github.com/donutnomad/dbmq/internal/repo/manualassignmentrepo"

	"github.com/gin-gonic/gin"
)

//go:embed all:static
var staticFS embed.FS

// ServerConfig 服务器配置
type ServerConfig struct {
	DB            interfaces.DB
	Port          int
	Host          string
	DashboardPath string            // Dashboard UI 挂载路径，默认 "/dbmq/api/v1/ui"
	APIHandler    []gin.HandlerFunc // API 前置中间件，传 nil 则无中间件
	AccessToken   string            // 访问令牌，为空则不校验
}

type Server struct {
	config    ServerConfig
	deps      *Deps
	engine    gin.IRouter
	server    *http.Server
	startTime time.Time
}

func newDeps(db interfaces.DB) *Deps {
	now := time.Now()
	return &Deps{
		DB:                   db,
		TopicQuery:           query.NewTopicQuery(db),
		ConsumerQuery:        query.NewConsumerQuery(db),
		MessageQuery:         query.NewMessageQuery(db),
		ClusterQuery:         query.NewClusterQuery(db),
		AdminClient:          dbmq.NewAdminClient(db),
		Producer:             dbmq.MustNewProducer(dbmq.ProducerConfig{DB: db}), // 创建 Producer 用于重发消息
		ManualAssignmentRepo: manualassignmentrepo.New(db),
		ConsumerGroupRepo:    consumergrouprepo.New(db),
		HeartbeatRepo:        heartbeatrepo.New(db),
		ProgressRepo:         consumerprogressrepo.New(db),
		StartTime:            func() int64 { return now.Unix() },
	}
}

func NewServer(config ServerConfig) (*Server, error) {
	db := config.DB
	if db == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if config.Port == 0 {
		config.Port = 8080
	}
	if config.Host == "" {
		config.Host = "localhost"
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(corsMiddleware())
	engine.Use(loggingMiddleware())
	return &Server{
		config:    config,
		startTime: time.Now(),
		deps:      newDeps(db),
		engine:    engine,
	}, nil
}

func (s *Server) RegisterAPIs() {
	registerAPIs(s.engine, s.config.DashboardPath, "/dbmq/api/v1/", s.config.AccessToken, s.deps, s.config.APIHandler...)
}

func (s *Server) Start() error {
	s.RegisterAPIs()

	addr := fmt.Sprintf("%s:%d", s.config.Host, s.config.Port)
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.engine.(*gin.Engine),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	slog.Info("starting DBMQ API server", "addr", addr)
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
func (s *Server) Engine() gin.IRoutes {
	return s.engine
}

// Deps 返回依赖（用于测试）
func (s *Server) Deps() *Deps {
	return s.deps
}
