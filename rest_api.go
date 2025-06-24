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
	adminClient, err := NewAdminClient(AdminConfig{DB: config.DB})
	if err != nil {
		return nil, fmt.Errorf("failed to create admin client: %w", err)
	}

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
