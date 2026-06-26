package dbmqapi

import (
	"context"
	"fmt"
	"time"

	"github.com/donutnomad/dbmq"
)

// HealthAPI 健康检查 API
// @TAG(Health)
type HealthAPI interface {
	// Health 健康检查
	// @GET(/health)
	Health(ctx context.Context) (HealthResp, error)
	// ActuatorHealth Spring Boot 兼容健康检查
	// @GET(/actuator/health)
	ActuatorHealth(ctx context.Context) (HealthResp, error)
	// ActuatorInfo Spring Boot 兼容信息接口
	// @GET(/actuator/info)
	ActuatorInfo(ctx context.Context) (InfoResp, error)
}

type healthAPI struct {
	deps *Deps
}

func NewHealthAPI(deps *Deps) HealthAPI {
	return &healthAPI{deps: deps}
}

func (a *healthAPI) Health(ctx context.Context) (HealthResp, error) {
	return a.checkHealth(ctx)
}

func (a *healthAPI) ActuatorHealth(ctx context.Context) (HealthResp, error) {
	return a.checkHealth(ctx)
}

func (a *healthAPI) checkHealth(ctx context.Context) (HealthResp, error) {
	sqlDB, err := a.deps.DB.DB()
	if err != nil {
		return HealthResp{}, fmt.Errorf("database connection error: %w", err)
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		return HealthResp{}, fmt.Errorf("database ping failed: %w", err)
	}

	return HealthResp{
		Status:    "UP",
		Timestamp: time.Now().Format(time.RFC3339),
		Components: map[string]HealthStatus{
			"database": {Status: "UP"},
		},
	}, nil
}

func (a *healthAPI) ActuatorInfo(ctx context.Context) (InfoResp, error) {
	return InfoResp{
		App: AppInfo{
			Name:        "DBMQ",
			Description: "Database-based Message Queue",
			Version:     dbmq.Version(),
		},
		Build: BuildInfo{
			Time:    time.Now().Format(time.RFC3339),
			Version: dbmq.Version(),
		},
	}, nil
}
