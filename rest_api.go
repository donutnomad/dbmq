package dbmq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"gorm.io/gorm"
)

// RestAPIConfig REST API配置
type RestAPIConfig struct {
	DB     *gorm.DB // 数据库连接
	Port   int      // 监听端口，默认8080
	Host   string   // 监听地址，默认localhost
	Prefix string   // API路径前缀，默认/api/v1
}

// RestAPIServer REST API服务器，兼容Kafka UI工具
// 提供标准的Kafka管理REST接口
type RestAPIServer struct {
	config        RestAPIConfig
	metricsClient *MetricsClient
	adminClient   *AdminClient
	server        *http.Server
}

// NewRestAPIServer 创建新的REST API服务器实例
func NewRestAPIServer(config RestAPIConfig) (*RestAPIServer, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	// 设置默认值
	if config.Port == 0 {
		config.Port = 8080
	}
	if config.Host == "" {
		config.Host = "localhost"
	}
	if config.Prefix == "" {
		config.Prefix = "/api/v1"
	}

	// 创建监控客户端
	metricsClient, err := NewMetricsClient(MetricsConfig{DB: config.DB})
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics client: %w", err)
	}

	// 创建管理客户端
	adminClient := NewAdminClient(config.DB)
	return &RestAPIServer{
		config:        config,
		metricsClient: metricsClient,
		adminClient:   adminClient,
	}, nil
}

// APIResponse 标准API响应格式
type APIResponse struct {
	Success bool        `json:"success"`           // 请求是否成功
	Data    interface{} `json:"data,omitempty"`    // 响应数据
	Error   string      `json:"error,omitempty"`   // 错误信息
	Message string      `json:"message,omitempty"` // 附加消息
}

// Start 启动REST API服务器
func (ras *RestAPIServer) Start() error {
	router := mux.NewRouter()

	// 添加CORS中间件
	router.Use(corsMiddleware)

	// 添加日志中间件
	router.Use(loggingMiddleware)

	// 注册API路由
	ras.registerRoutes(router)

	// 创建HTTP服务器
	addr := fmt.Sprintf("%s:%d", ras.config.Host, ras.config.Port)
	ras.server = &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	fmt.Printf("Starting DBMQ REST API server on %s\n", addr)
	return ras.server.ListenAndServe()
}

// Stop 停止REST API服务器
func (ras *RestAPIServer) Stop(ctx context.Context) error {
	if ras.server != nil {
		return ras.server.Shutdown(ctx)
	}
	return nil
}

// registerRoutes 注册所有API路由
func (ras *RestAPIServer) registerRoutes(router *mux.Router) {
	api := router.PathPrefix(ras.config.Prefix).Subrouter()

	// 健康检查接口
	api.HandleFunc("/health", ras.healthHandler).Methods("GET")
	api.HandleFunc("/actuator/health", ras.healthHandler).Methods("GET") // 兼容Spring Boot
	api.HandleFunc("/actuator/info", ras.infoHandler).Methods("GET")     // 兼容Spring Boot

	// 添加统计页面路由
	api.HandleFunc("/dashboard", ras.dashboardHandler).Methods("GET")
	api.HandleFunc("/dashboard/data", ras.dashboardDataHandler).Methods("GET")
	// 添加详情页面路由
	api.HandleFunc("/dashboard/topic/{topicName}", ras.topicDetailHandler).Methods("GET")
	api.HandleFunc("/dashboard/consumer-group/{groupId}", ras.consumerGroupDetailHandler).Methods("GET")

	// 集群信息接口（兼容Kafka UI）
	api.HandleFunc("/clusters", ras.getClustersHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/metrics", ras.getClusterMetricsHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/brokers", ras.getBrokersHandler).Methods("GET")

	// Topic管理接口（兼容Kafka UI）
	api.HandleFunc("/clusters/{clusterId}/topics", ras.getTopicsHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/topics", ras.createTopicHandler).Methods("POST")
	api.HandleFunc("/clusters/{clusterId}/topics/{topicName}", ras.getTopicHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/topics/{topicName}", ras.deleteTopicHandler).Methods("DELETE")
	api.HandleFunc("/clusters/{clusterId}/topics/{topicName}/metrics", ras.getTopicMetricsHandler).Methods("GET")

	// 消费组管理接口（兼容Kafka UI）
	api.HandleFunc("/clusters/{clusterId}/consumer-groups", ras.getConsumerGroupsHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/consumer-groups/{groupId}", ras.getConsumerGroupHandler).Methods("GET")
	api.HandleFunc("/clusters/{clusterId}/consumer-groups/{groupId}/metrics", ras.getConsumerGroupMetricsHandler).Methods("GET")

	// 兼容Kafka REST Proxy的接口
	api.HandleFunc("/topics", ras.listTopicsHandler).Methods("GET")
	api.HandleFunc("/topics/{topicName}", ras.getTopicInfoHandler).Methods("GET")
	api.HandleFunc("/topics/{topicName}/partitions", ras.getPartitionsHandler).Methods("GET")
	api.HandleFunc("/brokers", ras.listBrokersHandler).Methods("GET")

	// 扩展的DBMQ专用接口
	api.HandleFunc("/dbmq/stats", ras.getDBMQStatsHandler).Methods("GET")
	api.HandleFunc("/dbmq/topics/{topicName}/messages", ras.getTopicMessagesHandler).Methods("GET")
}

// 健康检查处理器
func (ras *RestAPIServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 检查数据库连接
	sqlDB, err := ras.config.DB.DB()
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Database connection error", err)
		return
	}

	if err := sqlDB.PingContext(ctx); err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Database ping failed", err)
		return
	}

	health := map[string]interface{}{
		"status":    "UP",
		"timestamp": time.Now().Format(time.RFC3339),
		"components": map[string]interface{}{
			"database": map[string]string{
				"status": "UP",
			},
		},
	}

	ras.writeSuccessResponse(w, health)
}

// 信息处理器
func (ras *RestAPIServer) infoHandler(w http.ResponseWriter, r *http.Request) {
	info := map[string]interface{}{
		"app": map[string]string{
			"name":        "DBMQ",
			"description": "Database-based Message Queue",
			"version":     "1.0.0",
		},
		"build": map[string]string{
			"time":    time.Now().Format(time.RFC3339),
			"version": "1.0.0",
		},
	}

	ras.writeSuccessResponse(w, info)
}

// 获取集群列表处理器
func (ras *RestAPIServer) getClustersHandler(w http.ResponseWriter, r *http.Request) {
	clusters := []map[string]interface{}{
		{
			"clusterId":   "dbmq-cluster",
			"name":        "DBMQ Cluster",
			"brokerCount": 1,
			"status":      "online",
		},
	}

	ras.writeSuccessResponse(w, clusters)
}

// 获取集群指标处理器
func (ras *RestAPIServer) getClusterMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	metrics, err := ras.metricsClient.GetClusterMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get cluster metrics", err)
		return
	}

	ras.writeSuccessResponse(w, metrics)
}

// 获取Broker列表处理器
func (ras *RestAPIServer) getBrokersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get broker metrics", err)
		return
	}

	brokers := []interface{}{brokerMetrics}
	ras.writeSuccessResponse(w, brokers)
}

// 获取Topic列表处理器
func (ras *RestAPIServer) getTopicsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topics, err := ras.metricsClient.GetAllTopicsMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get topics", err)
		return
	}

	ras.writeSuccessResponse(w, topics)
}

// 创建Topic处理器
func (ras *RestAPIServer) createTopicHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var req NewTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		ras.writeErrorResponse(w, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	err := ras.adminClient.CreateTopic(ctx, req)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to create topic", err)
		return
	}

	ras.writeSuccessResponse(w, map[string]string{
		"message": fmt.Sprintf("Topic %s created successfully", req.Name),
	})
}

// 获取单个Topic处理器
func (ras *RestAPIServer) getTopicHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	topic, err := ras.metricsClient.GetTopicMetrics(ctx, topicName)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(w, topic)
}

// 删除Topic处理器
func (ras *RestAPIServer) deleteTopicHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	err := ras.adminClient.DeleteTopics(ctx, []string{topicName})
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to delete topic", err)
		return
	}

	ras.writeSuccessResponse(w, map[string]string{
		"message": fmt.Sprintf("Topic %s deleted successfully", topicName),
	})
}

// 获取Topic指标处理器
func (ras *RestAPIServer) getTopicMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	metrics, err := ras.metricsClient.GetTopicMetrics(ctx, topicName)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(w, metrics)
}

// 获取消费组列表处理器
func (ras *RestAPIServer) getConsumerGroupsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	groups, err := ras.metricsClient.GetAllConsumerGroupsMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get consumer groups", err)
		return
	}

	ras.writeSuccessResponse(w, groups)
}

// 获取单个消费组处理器
func (ras *RestAPIServer) getConsumerGroupHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	groupId := vars["groupId"]

	group, err := ras.metricsClient.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Consumer group not found", err)
		return
	}

	ras.writeSuccessResponse(w, group)
}

// 获取消费组指标处理器
func (ras *RestAPIServer) getConsumerGroupMetricsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	groupId := vars["groupId"]

	metrics, err := ras.metricsClient.GetConsumerGroupMetrics(ctx, groupId)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Consumer group not found", err)
		return
	}

	ras.writeSuccessResponse(w, metrics)
}

// 兼容Kafka REST Proxy的Topic列表处理器
func (ras *RestAPIServer) listTopicsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	topicNames, err := ras.adminClient.ListTopics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to list topics", err)
		return
	}

	ras.writeSuccessResponse(w, topicNames)
}

// 兼容Kafka REST Proxy的Topic信息处理器
func (ras *RestAPIServer) getTopicInfoHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	descriptions, err := ras.adminClient.DescribeTopics(ctx, []string{topicName})
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	if desc, exists := descriptions[topicName]; exists {
		ras.writeSuccessResponse(w, desc)
	} else {
		ras.writeErrorResponse(w, http.StatusNotFound, "Topic not found", nil)
	}
}

// 获取分区信息处理器
func (ras *RestAPIServer) getPartitionsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	topicMetrics, err := ras.metricsClient.GetTopicMetrics(ctx, topicName)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(w, topicMetrics.Partitions)
}

// 兼容Kafka REST Proxy的Broker列表处理器
func (ras *RestAPIServer) listBrokersHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get brokers", err)
		return
	}

	brokers := []interface{}{
		map[string]interface{}{
			"broker_id": brokerMetrics.BrokerID,
			"host":      brokerMetrics.Host,
			"port":      brokerMetrics.Port,
		},
	}

	ras.writeSuccessResponse(w, brokers)
}

// DBMQ统计信息处理器
func (ras *RestAPIServer) getDBMQStatsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	clusterMetrics, err := ras.metricsClient.GetClusterMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get stats", err)
		return
	}

	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(ctx)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get broker stats", err)
		return
	}

	stats := map[string]interface{}{
		"cluster": clusterMetrics,
		"broker":  brokerMetrics,
		"system": map[string]interface{}{
			"uptime":    brokerMetrics.Uptime,
			"version":   brokerMetrics.Version,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}

	ras.writeSuccessResponse(w, stats)
}

// 获取Topic消息处理器
func (ras *RestAPIServer) getTopicMessagesHandler(w http.ResponseWriter, r *http.Request) {
	// ctx := r.Context() // 预留给将来的消息查询实现使用
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	// 解析查询参数
	query := r.URL.Query()
	partition := query.Get("partition")
	offset := query.Get("offset")
	limit := query.Get("limit")

	// 设置默认值
	limitInt := 10
	if limit != "" {
		if l, err := strconv.Atoi(limit); err == nil && l > 0 && l <= 100 {
			limitInt = l
		}
	}

	// 这里可以扩展实现消息查询逻辑
	// 目前返回基本信息，将来可以使用ctx进行数据库查询
	result := map[string]interface{}{
		"topic":     topicName,
		"partition": partition,
		"offset":    offset,
		"limit":     limitInt,
		"messages":  []interface{}{}, // 实际实现中这里会返回消息列表
		"note":      "Message browsing feature can be extended based on requirements",
	}

	ras.writeSuccessResponse(w, result)
}

// 写入成功响应
func (ras *RestAPIServer) writeSuccessResponse(w http.ResponseWriter, data interface{}) {
	response := APIResponse{
		Success: true,
		Data:    data,
	}
	ras.writeJSONResponse(w, http.StatusOK, response)
}

// 写入错误响应
func (ras *RestAPIServer) writeErrorResponse(w http.ResponseWriter, statusCode int, message string, err error) {
	response := APIResponse{
		Success: false,
		Message: message,
	}
	if err != nil {
		response.Error = err.Error()
	}
	ras.writeJSONResponse(w, statusCode, response)
}

// 写入JSON响应
func (ras *RestAPIServer) writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

// CORS中间件
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// 日志中间件
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// 创建响应记录器来捕获状态码
		recorder := &responseRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)
		fmt.Printf("[%s] %s %s %d %v\n",
			start.Format("2006-01-02 15:04:05"),
			r.Method,
			r.URL.Path,
			recorder.statusCode,
			duration)
	})
}

// 响应记录器，用于记录状态码
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *responseRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// 统计仪表板页面处理器
func (ras *RestAPIServer) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DBMQ 监控仪表板</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        .header {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            text-align: center;
        }

        .header h1 {
            color: #333;
            font-size: 2.5em;
            margin-bottom: 10px;
        }

        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 0.9em;
        }

        .status-online {
            background: #4CAF50;
            color: white;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .stat-card {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            text-align: center;
        }

        .stat-card h3 {
            color: #666;
            font-size: 1.1em;
            margin-bottom: 10px;
        }

        .stat-value {
            font-size: 2.5em;
            font-weight: bold;
            color: #333;
        }

        .content-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }

        .section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .section h2 {
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #eee;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
        }

        .table th,
        .table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 600;
            color: #555;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .refresh-info {
            text-align: center;
            color: #666;
            font-size: 0.9em;
            margin-top: 20px;
        }

        .metric-badge {
            display: inline-block;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: bold;
        }

        .metric-success {
            background: #d4edda;
            color: #155724;
        }

        .metric-warning {
            background: #fff3cd;
            color: #856404;
        }

        .metric-info {
            background: #d1ecf1;
            color: #0c5460;
        }

        @media (max-width: 768px) {
            .content-grid {
                grid-template-columns: 1fr;
            }
            
            .header h1 {
                font-size: 2em;
            }
            
            .stat-value {
                font-size: 2em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 DBMQ 监控仪表板</h1>
            <span class="status-badge status-online">系统运行中</span>
            <div style="margin-top: 10px; color: #666;">
                最后更新: <span id="lastUpdate">--</span>
            </div>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <h3>Topic 总数</h3>
                <div class="stat-value" id="topicCount">--</div>
            </div>
            <div class="stat-card">
                <h3>消费组总数</h3>
                <div class="stat-value" id="consumerGroupCount">--</div>
            </div>
            <div class="stat-card">
                <h3>总消息数</h3>
                <div class="stat-value" id="totalMessages">--</div>
            </div>
            <div class="stat-card">
                <h3>系统运行时间</h3>
                <div class="stat-value" id="uptime">--</div>
            </div>
        </div>

        <div class="content-grid">
            <div class="section">
                <h2>📋 Topic 列表</h2>
                <div id="topicsLoading" class="loading">
                    <div class="spinner"></div>
                    <div>加载 Topics 中...</div>
                </div>
                <div id="topicsContent" style="display: none;">
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Topic 名称</th>
                                <th>分区数</th>
                                <th>消息数</th>
                                <th>状态</th>
                            </tr>
                        </thead>
                        <tbody id="topicsTable">
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="section">
                <h2>👥 消费组列表</h2>
                <div id="consumersLoading" class="loading">
                    <div class="spinner"></div>
                    <div>加载消费组中...</div>
                </div>
                <div id="consumersContent" style="display: none;">
                    <table class="table">
                        <thead>
                            <tr>
                                <th>消费组 ID</th>
                                <th>状态</th>
                                <th>成员数</th>
                                <th>延迟</th>
                            </tr>
                        </thead>
                        <tbody id="consumersTable">
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="refresh-info">
            🔄 页面每 2 秒自动刷新一次
        </div>
    </div>

    <script>
        let refreshInterval;

        // 格式化数字显示
        function formatNumber(num) {
            if (num === undefined || num === null) return '--';
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            } else if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return num.toString();
        }

        // 格式化运行时间
        function formatUptime(seconds) {
            if (!seconds) return '--';
            const days = Math.floor(seconds / 86400);
            const hours = Math.floor((seconds % 86400) / 3600);
            const minutes = Math.floor((seconds % 3600) / 60);
            
            if (days > 0) {
                return days + '天 ' + hours + '小时';
            } else if (hours > 0) {
                return hours + '小时 ' + minutes + '分钟';
            } else {
                return minutes + '分钟';
            }
        }

        // 获取状态徽章
        function getStatusBadge(status) {
            const statusMap = {
                'active': { class: 'metric-success', text: '活跃' },
                'stable': { class: 'metric-success', text: '稳定' },
                'empty': { class: 'metric-info', text: '空闲' },
                'dead': { class: 'metric-warning', text: '停止' }
            };
            
            const statusInfo = statusMap[status] || { class: 'metric-info', text: status || '未知' };
            return '<span class="metric-badge ' + statusInfo.class + '">' + statusInfo.text + '</span>';
        }

        // 加载仪表板数据
        async function loadDashboardData() {
            try {
                const response = await fetch('/api/v1/dashboard/data');
                const result = await response.json();
                
                if (result.success) {
                    updateDashboard(result.data);
                } else {
                    console.error('Failed to load dashboard data:', result.error);
                }
            } catch (error) {
                console.error('Error loading dashboard data:', error);
            }
        }

        // 更新仪表板显示
        function updateDashboard(data) {
            // 更新统计数据
            document.getElementById('topicCount').textContent = data.topics ? data.topics.length : 0;
            document.getElementById('consumerGroupCount').textContent = data.consumerGroups ? data.consumerGroups.length : 0;
            
            let totalMessages = 0;
            if (data.topics) {
                data.topics.forEach(topic => {
                    if (topic.messageCount) {
                        totalMessages += topic.messageCount;
                    }
                });
            }
            document.getElementById('totalMessages').textContent = formatNumber(totalMessages);
            
            if (data.system && data.system.uptime) {
                document.getElementById('uptime').textContent = formatUptime(data.system.uptime);
            }

            // 更新 Topics 表格
            updateTopicsTable(data.topics || []);
            
            // 更新消费组表格
            updateConsumerGroupsTable(data.consumerGroups || []);
            
            // 更新最后更新时间
            document.getElementById('lastUpdate').textContent = new Date().toLocaleString('zh-CN');
        }

        // 更新 Topics 表格
        function updateTopicsTable(topics) {
            const tbody = document.getElementById('topicsTable');
            tbody.innerHTML = '';
            
            if (topics.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无 Topic 数据</td></tr>';
            } else {
                topics.forEach(topic => {
                    const topicName = topic.name || topic.topicName || '--';
                    const row = document.createElement('tr');
                    row.style.cursor = 'pointer';
                    row.onclick = function(e) {
                        e.preventDefault();
                        window.open('/api/v1/dashboard/topic/' + encodeURIComponent(topicName), '_blank');
                    };
                    row.innerHTML = 
                        '<td><a href="#" onclick="event.preventDefault(); window.open(\'/api/v1/dashboard/topic/' + encodeURIComponent(topicName) + '\', \'_blank\'); return false;" style="color: #007bff; text-decoration: none;">' + topicName + '</a></td>' +
                        '<td>' + (topic.partitionCount || topic.partitions?.length || '--') + '</td>' +
                        '<td>' + formatNumber(topic.messageCount || 0) + '</td>' +
                        '<td>' + getStatusBadge(topic.status || 'active') + '</td>';
                    tbody.appendChild(row);
                });
            }
            
            // 显示内容，隐藏加载动画
            document.getElementById('topicsLoading').style.display = 'none';
            document.getElementById('topicsContent').style.display = 'block';
        }

        // 更新消费组表格
        function updateConsumerGroupsTable(consumerGroups) {
            const tbody = document.getElementById('consumersTable');
            tbody.innerHTML = '';
            
            if (consumerGroups.length === 0) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无消费组数据</td></tr>';
            } else {
                consumerGroups.forEach(group => {
                    const groupId = group.groupId || group.name || '--';
                    const row = document.createElement('tr');
                    row.style.cursor = 'pointer';
                    row.onclick = function(e) {
                        e.preventDefault();
                        window.open('/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId), '_blank');
                    };
                    row.innerHTML = 
                        '<td><a href="#" onclick="event.preventDefault(); window.open(\'/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId) + '\', \'_blank\'); return false;" style="color: #007bff; text-decoration: none;">' + groupId + '</a></td>' +
                        '<td>' + getStatusBadge(group.state || group.status || 'unknown') + '</td>' +
                        '<td>' + (group.memberCount || group.members?.length || 0) + '</td>' +
                        '<td>' + formatNumber(group.lag || 0) + '</td>';
                    tbody.appendChild(row);
                });
            }
            
            // 显示内容，隐藏加载动画
            document.getElementById('consumersLoading').style.display = 'none';
            document.getElementById('consumersContent').style.display = 'block';
        }

        // 初始化页面
        function init() {
            loadDashboardData();
            
            // 设置定时刷新（2秒）
            refreshInterval = setInterval(loadDashboardData, 2000);
        }

        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', init);
        
        // 页面隐藏时清除定时器，显示时重新设置
        document.addEventListener('visibilitychange', function() {
            if (document.hidden) {
                if (refreshInterval) {
                    clearInterval(refreshInterval);
                }
            } else {
                loadDashboardData();
                refreshInterval = setInterval(loadDashboardData, 2000);
            }
        });
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// 仪表板数据处理器
func (ras *RestAPIServer) dashboardDataHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// 获取 Topics 数据
	topics, err := ras.metricsClient.GetAllTopicsMetrics(ctx)
	if err != nil {
		topics = []TopicMetrics{} // 如果出错，返回空数组而不是失败
	}

	// 获取消费组数据
	consumerGroups, err := ras.metricsClient.GetAllConsumerGroupsMetrics(ctx)
	if err != nil {
		consumerGroups = []ConsumerGroupMetrics{} // 如果出错，返回空数组而不是失败
	}

	// 获取系统信息
	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(ctx)
	var systemInfo map[string]interface{}
	if err == nil {
		systemInfo = map[string]interface{}{
			"uptime":  brokerMetrics.Uptime,
			"version": brokerMetrics.Version,
		}
	} else {
		systemInfo = map[string]interface{}{
			"uptime":  0,
			"version": "unknown",
		}
	}

	dashboardData := map[string]interface{}{
		"topics":         topics,
		"consumerGroups": consumerGroups,
		"system":         systemInfo,
		"timestamp":      time.Now().Format(time.RFC3339),
	}

	ras.writeSuccessResponse(w, dashboardData)
}

// Topic详情页面处理器
func (ras *RestAPIServer) topicDetailHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Topic 详情 - ` + topicName + `</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        .header {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .header h1 {
            color: #333;
            font-size: 2.5em;
            margin-bottom: 10px;
        }

        .back-button {
            display: inline-block;
            padding: 8px 16px;
            background: #007bff;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            margin-bottom: 15px;
        }

        .back-button:hover {
            background: #0056b3;
        }

        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .info-card {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .info-card h3 {
            color: #666;
            font-size: 1.1em;
            margin-bottom: 10px;
        }

        .info-value {
            font-size: 1.8em;
            font-weight: bold;
            color: #333;
        }

        .section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }

        .section h2 {
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #eee;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
        }

        .table th,
        .table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 600;
            color: #555;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>📋 Topic 详情</h1>
            <h2 style="color: #666; margin-top: 10px;">` + topicName + `</h2>
        </div>

        <div id="loading" class="loading">
            <div class="spinner"></div>
            <div>加载Topic详情中...</div>
        </div>

        <div id="content" style="display: none;">
            <div class="info-grid">
                <div class="info-card">
                    <h3>分区数</h3>
                    <div class="info-value" id="partitionCount">--</div>
                </div>
                <div class="info-card">
                    <h3>消息总数</h3>
                    <div class="info-value" id="messageCount">--</div>
                </div>
                <div class="info-card">
                    <h3>存储大小</h3>
                    <div class="info-value" id="sizeBytes">--</div>
                </div>
                <div class="info-card">
                    <h3>最新偏移量</h3>
                    <div class="info-value" id="latestOffset">--</div>
                </div>
            </div>

            <div class="section">
                <h2>🗂️ 分区详情</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>分区ID</th>
                            <th>最新偏移量(从0开始计数)</th>
                            <th>消息数</th>
                            <th>存储大小</th>
                        </tr>
                    </thead>
                    <tbody id="partitionsTable">
                    </tbody>
                </table>
            </div>

            <div class="section">
                <h2>⚙️ Topic配置</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>配置项</th>
                            <th>值</th>
                        </tr>
                    </thead>
                    <tbody id="configTable">
                    </tbody>
                </table>
            </div>
        </div>

        <div id="error" style="display: none;" class="error">
            <strong>错误:</strong> <span id="errorMessage"></span>
        </div>
    </div>

    <script>
        // 格式化数字显示
        function formatNumber(num) {
            if (num === undefined || num === null) return '--';
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            } else if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return num.toString();
        }

        // 格式化字节大小
        function formatBytes(bytes) {
            if (bytes === 0) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        // 加载Topic详情
        async function loadTopicDetail() {
            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/` + topicName + `');
                const result = await response.json();
                
                if (result.success) {
                    updateTopicDetail(result.data);
                } else {
                    showError(result.error || '获取Topic详情失败');
                }
            } catch (error) {
                showError('网络错误: ' + error.message);
            }
        }

        // 更新Topic详情显示
        function updateTopicDetail(data) {
            // 更新基本信息
            document.getElementById('partitionCount').textContent = data.partitionCount || 0;
            document.getElementById('messageCount').textContent = formatNumber(data.messageCount || 0);
            document.getElementById('sizeBytes').textContent = formatBytes(data.sizeBytes || 0);
            document.getElementById('latestOffset').textContent = formatNumber(data.latestOffset || 0);

            // 更新分区详情表格
            const partitionsTable = document.getElementById('partitionsTable');
            partitionsTable.innerHTML = '';
            
            if (data.partitions && data.partitions.length > 0) {
                data.partitions.forEach(partition => {
                    const row = document.createElement('tr');
                    row.innerHTML = 
                        '<td>' + partition.partition + '</td>' +
                        '<td>' + formatNumber(partition.latestOffset || 0) + '</td>' +
                        '<td>' + formatNumber(partition.messageCount || 0) + '</td>' +
                        '<td>' + formatBytes(partition.sizeBytes || 0) + '</td>';
                    partitionsTable.appendChild(row);
                });
            } else {
                partitionsTable.innerHTML = '<tr><td colspan="4" style="text-align: center; color: #666;">暂无分区数据</td></tr>';
            }

            // 更新配置表格
            const configTable = document.getElementById('configTable');
            configTable.innerHTML = '';
            
            if (data.config && Object.keys(data.config).length > 0) {
                Object.entries(data.config).forEach(([key, value]) => {
                    const row = document.createElement('tr');
                    row.innerHTML = 
                        '<td>' + key + '</td>' +
                        '<td>' + value + '</td>';
                    configTable.appendChild(row);
                });
            } else {
                configTable.innerHTML = '<tr><td colspan="2" style="text-align: center; color: #666;">暂无配置数据</td></tr>';
            }

            // 显示内容，隐藏加载动画
            document.getElementById('loading').style.display = 'none';
            document.getElementById('content').style.display = 'block';
        }

        // 显示错误
        function showError(message) {
            document.getElementById('errorMessage').textContent = message;
            document.getElementById('loading').style.display = 'none';
            document.getElementById('error').style.display = 'block';
        }

        // 页面加载完成后加载数据
        document.addEventListener('DOMContentLoaded', loadTopicDetail);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// 消费组详情页面处理器
func (ras *RestAPIServer) consumerGroupDetailHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	groupId := vars["groupId"]

	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>消费组详情 - ` + groupId + `</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        .header {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .header h1 {
            color: #333;
            font-size: 2.5em;
            margin-bottom: 10px;
        }

        .back-button {
            display: inline-block;
            padding: 8px 16px;
            background: #007bff;
            color: white;
            text-decoration: none;
            border-radius: 5px;
            margin-bottom: 15px;
        }

        .back-button:hover {
            background: #0056b3;
        }

        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }

        .info-card {
            background: rgba(255, 255, 255, 0.95);
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }

        .info-card h3 {
            color: #666;
            font-size: 1.1em;
            margin-bottom: 10px;
        }

        .info-value {
            font-size: 1.8em;
            font-weight: bold;
            color: #333;
        }

        .section {
            background: rgba(255, 255, 255, 0.95);
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            margin-bottom: 20px;
        }

        .section h2 {
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 2px solid #eee;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
        }

        .table th,
        .table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 600;
            color: #555;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }

        .spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }

        .metric-badge {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.9em;
            font-weight: bold;
        }

        .metric-success {
            background: #d4edda;
            color: #155724;
        }

        .metric-warning {
            background: #fff3cd;
            color: #856404;
        }

        .metric-info {
            background: #d1ecf1;
            color: #0c5460;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>👥 消费组详情</h1>
            <h2 style="color: #666; margin-top: 10px;">` + groupId + `</h2>
        </div>

        <div id="loading" class="loading">
            <div class="spinner"></div>
            <div>加载消费组详情中...</div>
        </div>

        <div id="content" style="display: none;">
            <div class="info-grid">
                <div class="info-card">
                    <h3>状态</h3>
                    <div class="info-value" id="state">--</div>
                </div>
                <div class="info-card">
                    <h3>成员数</h3>
                    <div class="info-value" id="memberCount">--</div>
                </div>
                <div class="info-card">
                    <h3>总延迟</h3>
                    <div class="info-value" id="lag">--</div>
                </div>
                <div class="info-card">
                    <h3>代际ID</h3>
                    <div class="info-value" id="generationId">--</div>
                </div>
            </div>

            <div class="section">
                <h2>🧑‍💼 消费者成员</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>消费者ID</th>
                            <th>客户端ID</th>
                            <th>主机</th>
                            <th>分配分区数</th>
                            <th>最后心跳</th>
                        </tr>
                    </thead>
                    <tbody id="membersTable">
                    </tbody>
                </table>
            </div>

            <div class="section">
                <h2>📊 分区延迟详情</h2>
                <table class="table">
                    <thead>
                        <tr>
                            <th>Topic</th>
                            <th>分区</th>
                            <th>当前偏移量</th>
                            <th>最新偏移量</th>
                            <th>延迟</th>
                        </tr>
                    </thead>
                    <tbody id="partitionLagsTable">
                    </tbody>
                </table>
            </div>

            <div class="section">
                <h2>📋 分配的Topics</h2>
                <div id="assignedTopics" style="padding: 10px;">
                    <span style="color: #666;">加载中...</span>
                </div>
            </div>
        </div>

        <div id="error" style="display: none;" class="error">
            <strong>错误:</strong> <span id="errorMessage"></span>
        </div>
    </div>

    <script>
        // 格式化数字显示
        function formatNumber(num) {
            if (num === undefined || num === null) return '--';
            if (num >= 1000000) {
                return (num / 1000000).toFixed(1) + 'M';
            } else if (num >= 1000) {
                return (num / 1000).toFixed(1) + 'K';
            }
            return num.toString();
        }

        // 获取状态徽章
        function getStatusBadge(status) {
            const statusMap = {
                'Active': { class: 'metric-success', text: '活跃' },
                'Stable': { class: 'metric-success', text: '稳定' },
                'Empty': { class: 'metric-info', text: '空闲' },
                'Dead': { class: 'metric-warning', text: '停止' }
            };
            
            const statusInfo = statusMap[status] || { class: 'metric-info', text: status || '未知' };
            return '<span class="metric-badge ' + statusInfo.class + '">' + statusInfo.text + '</span>';
        }

        // 格式化时间
        function formatTime(timeStr) {
            if (!timeStr) return '--';
            try {
                const date = new Date(timeStr);
                return date.toLocaleString('zh-CN');
            } catch (e) {
                return timeStr;
            }
        }

        // 加载消费组详情
        async function loadConsumerGroupDetail() {
            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/consumer-groups/` + groupId + `');
                const result = await response.json();
                
                if (result.success) {
                    updateConsumerGroupDetail(result.data);
                } else {
                    showError(result.error || '获取消费组详情失败');
                }
            } catch (error) {
                showError('网络错误: ' + error.message);
            }
        }

        // 更新消费组详情显示
        function updateConsumerGroupDetail(data) {
            // 更新基本信息
            document.getElementById('state').innerHTML = getStatusBadge(data.state);
            document.getElementById('memberCount').textContent = data.members ? data.members.length : 0;
            document.getElementById('lag').textContent = formatNumber(data.lag || 0);
            document.getElementById('generationId').textContent = data.generationId || '--';

            // 更新成员表格
            const membersTable = document.getElementById('membersTable');
            membersTable.innerHTML = '';
            
            if (data.members && data.members.length > 0) {
                data.members.forEach(member => {
                    const row = document.createElement('tr');
                    row.innerHTML = 
                        '<td>' + (member.consumerId || '--') + '</td>' +
                        '<td>' + (member.clientId || '--') + '</td>' +
                        '<td>' + (member.host || '--') + '</td>' +
                        '<td>' + (member.assignment ? member.assignment.length : 0) + '</td>' +
                        '<td>' + formatTime(member.lastHeartbeat) + '</td>';
                    membersTable.appendChild(row);
                });
            } else {
                membersTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">暂无成员数据</td></tr>';
            }

            // 更新分区延迟表格
            const partitionLagsTable = document.getElementById('partitionLagsTable');
            partitionLagsTable.innerHTML = '';
            
            if (data.partitionLags && data.partitionLags.length > 0) {
                data.partitionLags.forEach(lag => {
                    const row = document.createElement('tr');
                    row.innerHTML = 
                        '<td>' + (lag.topic || '--') + '</td>' +
                        '<td>' + (lag.partition !== undefined ? lag.partition : '--') + '</td>' +
                        '<td>' + formatNumber(lag.currentOffset) + '</td>' +
                        '<td>' + formatNumber(lag.latestOffset) + '</td>' +
                        '<td>' + formatNumber(lag.lag) + '</td>';
                    partitionLagsTable.appendChild(row);
                });
            } else {
                partitionLagsTable.innerHTML = '<tr><td colspan="5" style="text-align: center; color: #666;">暂无延迟数据</td></tr>';
            }

            // 更新分配的Topics
            const assignedTopicsDiv = document.getElementById('assignedTopics');
            if (data.assignedTopics && data.assignedTopics.length > 0) {
                assignedTopicsDiv.innerHTML = data.assignedTopics.map(topic => 
                    '<span class="metric-badge metric-info" style="margin: 5px;">' + topic + '</span>'
                ).join('');
            } else {
                assignedTopicsDiv.innerHTML = '<span style="color: #666;">暂无分配的Topics</span>';
            }

            // 显示内容，隐藏加载动画
            document.getElementById('loading').style.display = 'none';
            document.getElementById('content').style.display = 'block';
        }

        // 显示错误
        function showError(message) {
            document.getElementById('errorMessage').textContent = message;
            document.getElementById('loading').style.display = 'none';
            document.getElementById('error').style.display = 'block';
        }

        // 页面加载完成后加载数据
        document.addEventListener('DOMContentLoaded', loadConsumerGroupDetail);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}
