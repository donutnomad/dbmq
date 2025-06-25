package dbmq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/donutnomad/dbmq/types"
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
	startTime     time.Time // 服务启动时间
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

	ras.startTime = time.Now()
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
	// 添加Topic管理页面
	api.HandleFunc("/dashboard/topics/create", ras.topicCreatePageHandler).Methods("GET")
	api.HandleFunc("/dashboard/topics/manage", ras.topicManagePageHandler).Methods("GET")
	// 添加消息生产页面
	api.HandleFunc("/dashboard/producer", ras.messageProducerPageHandler).Methods("GET")

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
	ctx := r.Context()
	vars := mux.Vars(r)
	topicName := vars["topicName"]

	// 解析查询参数
	query := r.URL.Query()
	partitionStr := query.Get("partition")
	offsetStr := query.Get("offset")
	limitStr := query.Get("limit")
	searchKey := query.Get("search")
	fromTime := query.Get("from_time")
	toTime := query.Get("to_time")

	// 设置默认值
	limitInt := 50
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 500 {
			limitInt = l
		}
	}

	offsetInt := int64(0)
	if offsetStr != "" {
		if o, err := strconv.ParseInt(offsetStr, 10, 64); err == nil && o >= 0 {
			offsetInt = o
		}
	}

	var partitionInt *uint
	if partitionStr != "" {
		if p, err := strconv.ParseUint(partitionStr, 10, 32); err == nil {
			part := uint(p)
			partitionInt = &part
		}
	}

	messages, err := ras.getMessages(ctx, topicName, partitionInt, offsetInt, limitInt, searchKey, fromTime, toTime)
	if err != nil {
		ras.writeErrorResponse(w, http.StatusInternalServerError, "Failed to get messages", err)
		return
	}

	result := map[string]interface{}{
		"topic":     topicName,
		"partition": partitionStr,
		"offset":    offsetInt,
		"limit":     limitInt,
		"search":    searchKey,
		"messages":  messages,
		"total":     len(messages),
	}

	ras.writeSuccessResponse(w, result)
}

// getMessages 获取消息列表
func (ras *RestAPIServer) getMessages(ctx context.Context, topicName string, partition *uint, offset int64, limit int, searchKey, fromTime, toTime string) ([]map[string]interface{}, error) {
	var messages []types.Message
	query := ras.config.DB.WithContext(ctx).Where("topic = ?", topicName)

	// 分区过滤
	if partition != nil {
		query = query.Where("`partition` = ?", *partition)
	}

	// 偏移量过滤
	if offset > 0 {
		query = query.Where("offset >= ?", offset)
	}

	// 时间范围过滤
	if fromTime != "" {
		if t, err := time.Parse(time.RFC3339, fromTime); err == nil {
			query = query.Where("created_at >= ?", t)
		}
	}
	if toTime != "" {
		if t, err := time.Parse(time.RFC3339, toTime); err == nil {
			query = query.Where("created_at <= ?", t)
		}
	}

	// 关键字搜索
	if searchKey != "" {
		query = query.Where("(message_key LIKE ? OR body LIKE ?)", "%"+searchKey+"%", "%"+searchKey+"%")
	}

	// 排序和限制
	err := query.Order("created_at DESC").Limit(limit).Find(&messages).Error
	if err != nil {
		return nil, err
	}

	// 转换为响应格式
	result := make([]map[string]interface{}, len(messages))
	for i, msg := range messages {
		// 将字节数组转换为字符串
		var messageKey string
		if msg.MessageKey.Valid {
			messageKey = msg.MessageKey.String
		} else {
			messageKey = ""
		}

		// 将消息体字节转换为字符串
		messageValue := string(msg.Body)

		result[i] = map[string]interface{}{
			"id":        msg.ID,
			"topic":     msg.Topic,
			"partition": msg.Partition,
			"offset":    msg.PerPartitionOffset,
			"key":       messageKey,
			"value":     messageValue,
			"timestamp": msg.CreatedAt.Format(time.RFC3339),
			"size":      len(msg.Body),
			"headers":   msg.Headers,
		}
	}

	return result, nil
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            margin: 0;
            font-weight: 600;
        }

        .status-badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: 500;
        }

        .status-online {
            background: #e6f4ea;
            color: #1e7e34;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-bottom: 12px;
        }

        .stat-card {
            background: #fff;
            padding: 12px;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .stat-card h3 {
            color: #666;
            font-size: 13px;
            margin-bottom: 4px;
            font-weight: 500;
        }

        .stat-value {
            font-size: 20px;
            font-weight: 600;
            color: #1a1a1a;
        }

        .content-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 12px;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .section h2 {
            color: #1a1a1a;
            font-size: 15px;
            margin-bottom: 8px;
            padding-bottom: 8px;
            border-bottom: 1px solid #eee;
            font-weight: 600;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .table th,
        .table td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #eee;
            line-height: 1.4;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 500;
            color: #666;
            font-size: 12px;
            white-space: nowrap;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 24px;
            color: #666;
        }

        .spinner {
            border: 2px solid #f3f3f3;
            border-top: 2px solid #3498db;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            margin: 0 auto 12px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .refresh-info {
            text-align: center;
            color: #666;
            font-size: 12px;
            margin-top: 12px;
        }

        .metric-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }

        .metric-success {
            background: #e6f4ea;
            color: #1e7e34;
        }

        .metric-warning {
            background: #fff3cd;
            color: #856404;
        }

        .metric-info {
            background: #e1f0ff;
            color: #0056b3;
        }

        .btn-create, .btn-manage {
            display: inline-block;
            padding: 4px 8px;
            margin-left: 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            font-weight: normal;
            border: none;
            cursor: pointer;
            line-height: 1.4;
        }

        .btn-manage {
            background: #52c41a;
        }

        .btn-create:hover {
            background: #096dd9;
        }

        .btn-manage:hover {
            background: #389e0d;
        }

        @media (max-width: 768px) {
            .content-grid {
                grid-template-columns: 1fr;
            }
            
            .header h1 {
                font-size: 16px;
            }
            
            .stat-value {
                font-size: 18px;
            }

            body {
                padding: 8px;
            }
        }

        .table-container {
            max-height: 600px;
            overflow-y: auto;
            margin-top: 8px;
        }

        .table-container::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        .table-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb {
            background: #ccc;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb:hover {
            background: #999;
        }

        .topic-link {
            color: #1a1a1a;
            text-decoration: none;
        }

        .topic-link:hover {
            color: #1890ff;
            text-decoration: underline;
        }

        .refresh-toggle {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            font-size: 12px;
            color: #666;
        }

        .toggle-switch {
            position: relative;
            display: inline-block;
            width: 40px;
            height: 20px;
        }

        .toggle-switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }

        .toggle-slider {
            position: absolute;
            cursor: pointer;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: #ccc;
            transition: .4s;
            border-radius: 20px;
        }

        .toggle-slider:before {
            position: absolute;
            content: "";
            height: 16px;
            width: 16px;
            left: 2px;
            bottom: 2px;
            background-color: white;
            transition: .4s;
            border-radius: 50%;
        }

        input:checked + .toggle-slider {
            background-color: #1890ff;
        }

        input:checked + .toggle-slider:before {
            transform: translateX(20px);
        }

    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <h1>DBMQ 监控仪表板</h1>
                <span class="status-badge status-online">系统运行中</span>
            </div>
            <div style="display: flex; align-items: center; gap: 16px;">
                <div class="refresh-toggle">
                    <label class="toggle-switch">
                        <input type="checkbox" id="autoRefreshToggle" checked>
                        <span class="toggle-slider"></span>
                    </label>
                    <span>自动刷新</span>
                </div>
                <div style="color: #666; font-size: 12px;">
                    最后更新: <span id="lastUpdate">--</span>
                </div>
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
                <h2>
                    <span>Topic 列表</span>
                    <div>
                        <a href="/api/v1/dashboard/topics/create" class="btn-create">创建Topic</a>
                        <a href="/api/v1/dashboard/topics/manage" class="btn-manage">管理</a>
                    </div>
                </h2>
                <div id="topicsLoading" class="loading">
                    <div class="spinner"></div>
                    <div>加载 Topics 中...</div>
                </div>
                <div id="topicsContent" style="display: none;">
                    <div class="table-container">
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
            </div>

            <div class="section">
                <h2>
                    <span>消费组列表</span>
                    <a href="/api/v1/dashboard/producer" class="btn-create">发送消息</a>
                </h2>
                <div id="consumersLoading" class="loading">
                    <div class="spinner"></div>
                    <div>加载消费组中...</div>
                </div>
                <div id="consumersContent" style="display: none;">
                    <div class="table-container">
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
        </div>

        <div class="refresh-info">
            <span id="refreshStatus">页面每 5 秒自动刷新一次</span>
        </div>
    </div>

    <script>
        let refreshInterval;
        let isAutoRefreshEnabled = true;
        let chartData = {
            timestamps: []
        };

        // 切换自动刷新状态
        function toggleAutoRefresh(enabled) {
            isAutoRefreshEnabled = enabled;
            if (enabled) {
                loadDashboardData();
                refreshInterval = setInterval(loadDashboardData, 5000);
                document.getElementById('refreshStatus').textContent = '页面每 5 秒自动刷新一次';
            } else {
                if (refreshInterval) {
                    clearInterval(refreshInterval);
                }
                document.getElementById('refreshStatus').textContent = '自动刷新已关闭';
            }
        }

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
                        window.location.href = '/api/v1/dashboard/topic/' + encodeURIComponent(topicName);
                    };
                    row.innerHTML = 
                        '<td><a href="/api/v1/dashboard/topic/' + encodeURIComponent(topicName) + '" class="topic-link" onclick="event.stopPropagation()">' + topicName + '</a></td>' +
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
                        window.location.href = '/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId);
                    };
                    row.innerHTML = 
                        '<td><a href="/api/v1/dashboard/consumer-group/' + encodeURIComponent(groupId) + '" class="topic-link" onclick="event.stopPropagation()">' + groupId + '</a></td>' +
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
            
            // 设置定时刷新（5秒）
            refreshInterval = setInterval(loadDashboardData, 5000);

            // 绑定自动刷新开关事件
            document.getElementById('autoRefreshToggle').addEventListener('change', function(e) {
                toggleAutoRefresh(e.target.checked);
            });
        }

        // 页面加载完成后初始化
        document.addEventListener('DOMContentLoaded', init);
        
        // 页面隐藏时清除定时器，显示时重新设置
        document.addEventListener('visibilitychange', function() {
            const autoRefreshToggle = document.getElementById('autoRefreshToggle');
            if (document.hidden) {
                if (refreshInterval) {
                    clearInterval(refreshInterval);
                }
            } else {
                if (autoRefreshToggle.checked) {
                    loadDashboardData();
                    refreshInterval = setInterval(loadDashboardData, 5000);
                }
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
	uptime := time.Since(ras.startTime).Seconds()
	systemInfo := map[string]interface{}{
		"uptime":  uptime,
		"version": "1.0.0",
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .back-button {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 8px;
            line-height: 1.4;
        }

        .back-button:hover {
            background: #096dd9;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            margin-bottom: 4px;
            font-weight: 600;
        }

        .header h2 {
            color: #666;
            font-size: 14px;
            font-weight: normal;
        }

        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-bottom: 12px;
        }

        .info-card {
            background: #fff;
            padding: 12px;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .info-card h3 {
            color: #666;
            font-size: 13px;
            margin-bottom: 4px;
            font-weight: 500;
        }

        .info-value {
            font-size: 20px;
            font-weight: 600;
            color: #1a1a1a;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 12px;
        }

        .section h2 {
            color: #1a1a1a;
            font-size: 15px;
            margin-bottom: 8px;
            padding-bottom: 8px;
            border-bottom: 1px solid #eee;
            font-weight: 600;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .table th,
        .table td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #eee;
            line-height: 1.4;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 500;
            color: #666;
            font-size: 12px;
            white-space: nowrap;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 24px;
            color: #666;
        }

        .spinner {
            border: 2px solid #f3f3f3;
            border-top: 2px solid #3498db;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            margin: 0 auto 12px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error {
            background: #fff2f0;
            border: 1px solid #ffccc7;
            color: #cf1322;
            padding: 12px;
            border-radius: 4px;
            margin: 12px 0;
            font-size: 13px;
        }

        .message-controls {
            display: flex;
            gap: 12px;
            margin-bottom: 12px;
            padding: 12px;
            background: #f8f9fa;
            border-radius: 4px;
            flex-wrap: wrap;
            align-items: center;
        }

        .control-group {
            display: flex;
            align-items: center;
            gap: 4px;
        }

        .control-group label {
            font-weight: 500;
            color: #666;
            font-size: 12px;
        }

        .control-group input, .control-group select {
            padding: 4px 8px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            font-size: 12px;
            min-width: 100px;
        }

        .btn-primary, .btn-secondary {
            padding: 4px 8px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 500;
            line-height: 1.4;
        }

        .btn-primary {
            background: #1890ff;
            color: white;
        }

        .btn-primary:hover {
            background: #096dd9;
        }

        .btn-secondary {
            background: #f5f5f5;
            color: #595959;
            border: 1px solid #d9d9d9;
        }

        .btn-secondary:hover {
            background: #e8e8e8;
        }

        .table-container {
            max-height: 400px;
            overflow-y: auto;
            margin-top: 8px;
        }

        .table-container::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        .table-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb {
            background: #ccc;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb:hover {
            background: #999;
        }

        .message-item {
            border: 1px solid #f0f0f0;
            border-radius: 4px;
            margin-bottom: 8px;
            background: white;
        }

        .message-header {
            background: #fafafa;
            padding: 8px 12px;
            border-bottom: 1px solid #f0f0f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
            cursor: pointer;
            font-size: 12px;
        }

        .message-header:hover {
            background: #f5f5f5;
        }

        .message-meta {
            display: flex;
            gap: 16px;
            color: #666;
            font-size: 12px;
        }

        .message-content {
            padding: 12px;
            display: none;
        }

        .message-content.expanded {
            display: block;
        }

        .message-value {
            background: #f8f9fa;
            padding: 8px;
            border-radius: 4px;
            font-family: 'SFMono-Regular', Consolas, 'Liberation Mono', Menlo, Courier, monospace;
            font-size: 12px;
            white-space: pre-wrap;
            word-break: break-all;
            max-height: 300px;
            overflow-y: auto;
        }

        .message-key {
            font-weight: 500;
            color: #1890ff;
        }

        .no-messages {
            text-align: center;
            padding: 24px;
            color: #666;
            background: #fafafa;
            border-radius: 4px;
            font-size: 13px;
        }

        @media (max-width: 768px) {
            body {
                padding: 8px;
            }

            .info-grid {
                grid-template-columns: 1fr;
            }

            .message-controls {
                flex-direction: column;
                align-items: stretch;
            }

            .control-group {
                flex-direction: column;
                align-items: stretch;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>Topic 详情</h1>
            <h2>` + topicName + `</h2>
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
                <h2>分区详情</h2>
                <div class="table-container">
                    <table class="table">
                        <thead>
                            <tr>
                                <th>分区ID</th>
                                <th>最新偏移量</th>
                                <th>消息数</th>
                                <th>存储大小</th>
                            </tr>
                        </thead>
                        <tbody id="partitionsTable">
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="section">
                <h2>Topic配置</h2>
                <div class="table-container">
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

            <div class="section">
                <h2>消息浏览器</h2>
                <div class="message-controls">
                    <div class="control-group">
                        <label>分区:</label>
                        <select id="partitionSelect">
                            <option value="">所有分区</option>
                        </select>
                    </div>
                    <div class="control-group">
                        <label>搜索:</label>
                        <input type="text" id="searchInput" placeholder="搜索消息键或内容...">
                    </div>
                    <div class="control-group">
                        <label>数量:</label>
                        <select id="limitSelect">
                            <option value="20">20</option>
                            <option value="50" selected>50</option>
                            <option value="100">100</option>
                            <option value="200">200</option>
                        </select>
                    </div>
                    <button id="searchBtn" class="btn-primary">查询消息</button>
                    <button id="refreshBtn" class="btn-secondary">刷新</button>
                </div>

                <div id="messagesContainer">
                    <div class="loading" id="messagesLoading" style="display: none;">
                        <div class="spinner"></div>
                        <div>加载消息中...</div>
                    </div>
                    <div id="messagesTable"></div>
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

            // 初始化分区选择器
            initializePartitionSelect(data.partitions);

            // 显示内容，隐藏加载动画
            document.getElementById('loading').style.display = 'none';
            document.getElementById('content').style.display = 'block';

            // 自动加载最新消息
            loadMessages();
        }

        // 显示错误
        function showError(message) {
            document.getElementById('errorMessage').textContent = message;
            document.getElementById('loading').style.display = 'none';
            document.getElementById('error').style.display = 'block';
        }

        // 初始化分区选择器
        function initializePartitionSelect(partitions) {
            const partitionSelect = document.getElementById('partitionSelect');
            partitionSelect.innerHTML = '<option value="">所有分区</option>';
            
            if (partitions && partitions.length > 0) {
                partitions.forEach(partition => {
                    const option = document.createElement('option');
                    option.value = partition.partition;
                    option.textContent = '分区 ' + partition.partition;
                    partitionSelect.appendChild(option);
                });
            }
        }

        // 加载消息
        async function loadMessages() {
            const messagesLoading = document.getElementById('messagesLoading');
            const messagesTable = document.getElementById('messagesTable');
            
            messagesLoading.style.display = 'block';
            messagesTable.innerHTML = '';

            try {
                const partition = document.getElementById('partitionSelect').value;
                const search = document.getElementById('searchInput').value;
                const limit = document.getElementById('limitSelect').value;

                let url = '/api/v1/dbmq/topics/` + topicName + `/messages?limit=' + limit;
                if (partition) url += '&partition=' + partition;
                if (search) url += '&search=' + encodeURIComponent(search);

                const response = await fetch(url);
                const result = await response.json();
                
                if (result.success) {
                    displayMessages(result.data.messages);
                } else {
                    showError(result.error || '获取消息失败');
                }
            } catch (error) {
                showError('网络错误: ' + error.message);
            } finally {
                messagesLoading.style.display = 'none';
            }
        }

        // 显示消息列表
        function displayMessages(messages) {
            const messagesTable = document.getElementById('messagesTable');
            
            if (!messages || messages.length === 0) {
                messagesTable.innerHTML = '<div class="no-messages">📭 暂无消息数据</div>';
                return;
            }

            let html = '';
            messages.forEach((msg, index) => {
                const timestamp = new Date(msg.timestamp).toLocaleString('zh-CN');
                const msgKey = msg.key || '(无键)';
                const msgValue = msg.value || '';
                html += '<div class="message-item">' +
                    '<div class="message-header" onclick="toggleMessage(' + index + ')">' +
                        '<div>' +
                            '<span class="message-key">' + msgKey + '</span>' +
                            '<div class="message-meta">' +
                                '<span>分区: ' + msg.partition + '</span>' +
                                '<span>偏移量: ' + msg.offset + '</span>' +
                                '<span>时间: ' + timestamp + '</span>' +
                                '<span>大小: ' + formatBytes(msg.size) + '</span>' +
                            '</div>' +
                        '</div>' +
                        '<span id="toggle-' + index + '">🔼</span>' +
                    '</div>' +
                    '<div class="message-content expanded" id="content-' + index + '">' +
                        '<div class="message-value">' + msgValue + '</div>' +
                    '</div>' +
                '</div>';
            });
            
            messagesTable.innerHTML = html;
        }

        // 切换消息展开/折叠
        function toggleMessage(index) {
            const content = document.getElementById('content-' + index);
            const toggle = document.getElementById('toggle-' + index);
            
            if (content.classList.contains('expanded')) {
                content.classList.remove('expanded');
                toggle.textContent = '🔽';
            } else {
                content.classList.add('expanded');
                toggle.textContent = '🔼';
            }
        }

        // 绑定事件监听器
        document.addEventListener('DOMContentLoaded', function() {
            loadTopicDetail();

            // 搜索按钮事件
            document.getElementById('searchBtn').addEventListener('click', loadMessages);
            
            // 刷新按钮事件
            document.getElementById('refreshBtn').addEventListener('click', loadMessages);
            
            // 回车搜索
            document.getElementById('searchInput').addEventListener('keypress', function(e) {
                if (e.key === 'Enter') {
                    loadMessages();
                }
            });
        });
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
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .back-button {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 8px;
            line-height: 1.4;
        }

        .back-button:hover {
            background: #096dd9;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            margin-bottom: 4px;
            font-weight: 600;
        }

        .header h2 {
            color: #666;
            font-size: 14px;
            font-weight: normal;
        }

        .info-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 12px;
            margin-bottom: 12px;
        }

        .info-card {
            background: #fff;
            padding: 12px;
            border-radius: 6px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .info-card h3 {
            color: #666;
            font-size: 13px;
            margin-bottom: 4px;
            font-weight: 500;
        }

        .info-value {
            font-size: 20px;
            font-weight: 600;
            color: #1a1a1a;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 12px;
        }

        .section h2 {
            color: #1a1a1a;
            font-size: 15px;
            margin-bottom: 8px;
            padding-bottom: 8px;
            border-bottom: 1px solid #eee;
            font-weight: 600;
        }

        .table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .table th,
        .table td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #eee;
            line-height: 1.4;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 500;
            color: #666;
            font-size: 12px;
            white-space: nowrap;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .loading {
            text-align: center;
            padding: 24px;
            color: #666;
        }

        .spinner {
            border: 2px solid #f3f3f3;
            border-top: 2px solid #3498db;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            margin: 0 auto 12px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error {
            background: #fff2f0;
            border: 1px solid #ffccc7;
            color: #cf1322;
            padding: 12px;
            border-radius: 4px;
            margin: 12px 0;
            font-size: 13px;
        }

        .metric-badge {
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }

        .metric-success {
            background: #e6f4ea;
            color: #1e7e34;
        }

        .metric-warning {
            background: #fff3cd;
            color: #856404;
        }

        .metric-info {
            background: #e1f0ff;
            color: #0056b3;
        }

        .table-container {
            max-height: 400px;
            overflow-y: auto;
            margin-top: 8px;
        }

        .table-container::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        .table-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb {
            background: #ccc;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb:hover {
            background: #999;
        }

        .assigned-topics {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            padding: 12px;
            background: #fafafa;
            border-radius: 4px;
        }

        .topic-badge {
            display: inline-block;
            padding: 4px 8px;
            background: #e1f0ff;
            color: #0056b3;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }

        @media (max-width: 768px) {
            body {
                padding: 8px;
            }

            .info-grid {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>消费组详情</h1>
            <h2>` + groupId + `</h2>
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
                <h2>消费者成员</h2>
                <div class="table-container">
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
            </div>

            <div class="section">
                <h2>分区延迟详情</h2>
                <div class="table-container">
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
            </div>

            <div class="section">
                <h2>分配的Topics</h2>
                <div id="assignedTopics" class="assigned-topics">
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
                    '<span class="topic-badge">' + topic + '</span>'
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

// Topic创建页面处理器
func (ras *RestAPIServer) topicCreatePageHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>创建 Topic - DBMQ</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 800px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .back-button {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 8px;
            line-height: 1.4;
        }

        .back-button:hover {
            background: #096dd9;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            font-weight: 600;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 16px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .form-group {
            margin-bottom: 16px;
        }

        .form-group label {
            display: block;
            margin-bottom: 4px;
            font-weight: 500;
            color: #1a1a1a;
            font-size: 13px;
        }

        .form-group input, 
        .form-group textarea {
            width: 100%;
            padding: 6px 8px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            font-size: 13px;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .form-group input:hover, 
        .form-group textarea:hover {
            border-color: #40a9ff;
        }

        .form-group input:focus, 
        .form-group textarea:focus {
            border-color: #1890ff;
            outline: none;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
        }

        .form-group small {
            color: #666;
            font-size: 12px;
            margin-top: 4px;
            display: block;
        }

        .btn-primary {
            background: #1890ff;
            color: white;
            padding: 6px 12px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            font-weight: 500;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .btn-primary:hover {
            background: #096dd9;
        }

        .btn-primary:disabled {
            background: #bfbfbf;
            cursor: not-allowed;
        }

        .btn-secondary {
            background: #f5f5f5;
            color: #595959;
            padding: 6px 12px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            margin-left: 8px;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .btn-secondary:hover {
            background: #e8e8e8;
        }

        .alert {
            padding: 8px 12px;
            margin-bottom: 12px;
            border-radius: 4px;
            font-size: 13px;
            line-height: 1.4;
        }

        .alert-success {
            background: #e6f4ea;
            color: #1e7e34;
            border: 1px solid #b7dfb9;
        }

        .alert-error {
            background: #fff2f0;
            color: #cf1322;
            border: 1px solid #ffccc7;
        }

        @media (max-width: 768px) {
            body {
                padding: 8px;
            }

            .container {
                width: 100%;
            }

            .form-group input,
            .form-group textarea {
                font-size: 16px; /* 防止iOS缩放 */
            }

            .btn-secondary {
                margin-left: 0;
                margin-top: 8px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>创建新 Topic</h1>
        </div>

        <div class="section">
            <div id="message" style="display: none;"></div>
            
            <form id="createTopicForm">
                <div class="form-group">
                    <label for="topicName">Topic 名称 *</label>
                    <input type="text" id="topicName" name="topicName" required>
                    <small>Topic名称，只能包含字母、数字、下划线和连字符</small>
                </div>

                <div class="form-group">
                    <label for="partitions">分区数 *</label>
                    <input type="number" id="partitions" name="partitions" min="1" max="100" value="3" required>
                    <small>分区数量，建议根据预期的并发消费者数量设置</small>
                </div>

                <div class="form-group">
                    <label for="retentionHours">保留时间（小时）</label>
                    <input type="number" id="retentionHours" name="retentionHours" min="1" value="168">
                    <small>消息保留时间，默认168小时（7天）</small>
                </div>

                <div class="form-group">
                    <label for="description">描述</label>
                    <textarea id="description" name="description" rows="3"></textarea>
                    <small>Topic的描述信息（可选）</small>
                </div>

                <button type="submit" class="btn-primary">创建 Topic</button>
                <button type="button" class="btn-secondary" onclick="window.location.href='/api/v1/dashboard'">取消</button>
            </form>
        </div>
    </div>

    <script>
        document.getElementById('createTopicForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const formData = new FormData(e.target);
            const topicData = {
                name: formData.get('topicName'),
                numPartitions: parseInt(formData.get('partitions')),
                config: {
                    'retention.hours': parseInt(formData.get('retentionHours') || 168)
                }
            };

            if (formData.get('description')) {
                topicData.description = formData.get('description');
            }

            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify(topicData)
                });

                const result = await response.json();
                
                if (result.success) {
                    showMessage('Topic 创建成功！', 'success');
                    setTimeout(() => {
                        window.location.href = '/api/v1/dashboard';
                    }, 2000);
                } else {
                    showMessage('创建失败: ' + (result.error || '未知错误'), 'error');
                }
            } catch (error) {
                showMessage('网络错误: ' + error.message, 'error');
            }
        });

        function showMessage(text, type) {
            const messageDiv = document.getElementById('message');
            messageDiv.className = 'alert alert-' + type;
            messageDiv.textContent = text;
            messageDiv.style.display = 'block';
            
            if (type === 'success') {
                setTimeout(() => {
                    messageDiv.style.display = 'none';
                }, 5000);
            }
        }
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// Topic管理页面处理器
func (ras *RestAPIServer) topicManagePageHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Topic 管理 - DBMQ</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .header-left {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .back-button {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            line-height: 1.4;
        }

        .back-button:hover {
            background: #096dd9;
        }

        .btn-create {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #52c41a;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            line-height: 1.4;
        }

        .btn-create:hover {
            background: #389e0d;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            font-weight: 600;
            margin-left: 12px;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .table {
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }

        .table th,
        .table td {
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #eee;
            line-height: 1.4;
        }

        .table th {
            background: #f8f9fa;
            font-weight: 500;
            color: #666;
            font-size: 12px;
            white-space: nowrap;
        }

        .table tr:hover {
            background: #f8f9fa;
        }

        .btn-danger {
            background: #ff4d4f;
            color: white;
            padding: 4px 8px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .btn-danger:hover {
            background: #cf1322;
        }

        .loading {
            text-align: center;
            padding: 24px;
            color: #666;
        }

        .spinner {
            border: 2px solid #f3f3f3;
            border-top: 2px solid #3498db;
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            margin: 0 auto 12px;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .table-container {
            max-height: 600px;
            overflow-y: auto;
            margin-top: 8px;
        }

        .table-container::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }

        .table-container::-webkit-scrollbar-track {
            background: #f1f1f1;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb {
            background: #ccc;
            border-radius: 3px;
        }

        .table-container::-webkit-scrollbar-thumb:hover {
            background: #999;
        }

        .topic-link {
            color: #1a1a1a;
            text-decoration: none;
        }

        .topic-link:hover {
            color: #1890ff;
            text-decoration: underline;
        }

        @media (max-width: 768px) {
            body {
                padding: 8px;
            }

            .header {
                flex-direction: column;
                align-items: flex-start;
                gap: 8px;
            }

            .header-left {
                width: 100%;
            }

            .btn-create {
                margin-top: 8px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-left">
                <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
                <h1>Topic 管理</h1>
            </div>
            <a href="/api/v1/dashboard/topics/create" class="btn-create">+ 创建新 Topic</a>
        </div>

        <div class="section">
            <div id="loading" class="loading">
                <div class="spinner"></div>
                <div>加载 Topics 中...</div>
            </div>

            <div id="content" style="display: none;">
                <div class="table-container">
                    <table class="table">
                        <thead>
                            <tr>
                                <th>Topic 名称</th>
                                <th>分区数</th>
                                <th>消息数</th>
                                <th>存储大小</th>
                                <th>创建时间</th>
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody id="topicsTable">
                        </tbody>
                    </table>
                </div>
            </div>
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
            if (bytes === 0 || !bytes) return '0 B';
            const k = 1024;
            const sizes = ['B', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }

        // 加载Topics
        async function loadTopics() {
            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics');
                const result = await response.json();
                
                if (result.success) {
                    displayTopics(result.data);
                } else {
                    alert('获取Topics失败: ' + (result.error || '未知错误'));
                }
            } catch (error) {
                alert('网络错误: ' + error.message);
            } finally {
                document.getElementById('loading').style.display = 'none';
                document.getElementById('content').style.display = 'block';
            }
        }

        // 显示Topics
        function displayTopics(topics) {
            const tbody = document.getElementById('topicsTable');
            tbody.innerHTML = '';
            
            if (!topics || topics.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" style="text-align: center; color: #666;">暂无 Topic 数据</td></tr>';
                return;
            }

            topics.forEach(topic => {
                const row = document.createElement('tr');
                const topicName = topic.name || topic.topicName || '--';
                row.innerHTML = 
                    '<td><a href="/api/v1/dashboard/topic/' + encodeURIComponent(topicName) + '" class="topic-link">' + topicName + '</a></td>' +
                    '<td>' + (topic.partitionCount || '--') + '</td>' +
                    '<td>' + formatNumber(topic.messageCount || 0) + '</td>' +
                    '<td>' + formatBytes(topic.sizeBytes || 0) + '</td>' +
                    '<td>' + (topic.createdAt ? new Date(topic.createdAt).toLocaleString('zh-CN') : '--') + '</td>' +
                    '<td><button class="btn-danger" onclick="deleteTopic(\'' + topicName + '\')">删除</button></td>';
                tbody.appendChild(row);
            });
        }

        // 删除Topic
        async function deleteTopic(topicName) {
            if (!confirm('确定要删除 Topic "' + topicName + '" 吗？此操作不可撤销！')) {
                return;
            }

            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/' + encodeURIComponent(topicName), {
                    method: 'DELETE'
                });

                const result = await response.json();
                
                if (result.success) {
                    alert('Topic 删除成功！');
                    loadTopics(); // 重新加载列表
                } else {
                    alert('删除失败: ' + (result.error || '未知错误'));
                }
            } catch (error) {
                alert('网络错误: ' + error.message);
            }
        }

        // 页面加载完成后加载数据
        document.addEventListener('DOMContentLoaded', loadTopics);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}

// 消息生产页面处理器
func (ras *RestAPIServer) messageProducerPageHandler(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>消息生产器 - DBMQ</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            background: #f0f2f5;
            min-height: 100vh;
            padding: 12px;
            font-size: 13px;
            line-height: 1.4;
        }

        .container {
            max-width: 800px;
            margin: 0 auto;
        }

        .header {
            background: #fff;
            padding: 12px 16px;
            border-radius: 6px;
            margin-bottom: 12px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
        }

        .back-button {
            display: inline-flex;
            align-items: center;
            padding: 4px 8px;
            background: #1890ff;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 8px;
            line-height: 1.4;
        }

        .back-button:hover {
            background: #096dd9;
        }

        .header h1 {
            color: #1a1a1a;
            font-size: 18px;
            font-weight: 600;
        }

        .header p {
            color: #666;
            font-size: 13px;
            margin-top: 4px;
        }

        .section {
            background: #fff;
            border-radius: 6px;
            padding: 16px;
            box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
            margin-bottom: 12px;
        }

        .form-group {
            margin-bottom: 16px;
        }

        .form-group label {
            display: block;
            margin-bottom: 4px;
            font-weight: 500;
            color: #1a1a1a;
            font-size: 13px;
        }

        .form-group input,
        .form-group select,
        .form-group textarea {
            width: 100%;
            padding: 6px 8px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            font-size: 13px;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .form-group input:hover,
        .form-group select:hover,
        .form-group textarea:hover {
            border-color: #40a9ff;
        }

        .form-group input:focus,
        .form-group select:focus,
        .form-group textarea:focus {
            border-color: #1890ff;
            outline: none;
            box-shadow: 0 0 0 2px rgba(24, 144, 255, 0.2);
        }

        .form-group small {
            color: #666;
            font-size: 12px;
            margin-top: 4px;
            display: block;
        }

        .form-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
        }

        .btn-primary {
            background: #1890ff;
            color: white;
            padding: 6px 12px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            font-weight: 500;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .btn-primary:hover {
            background: #096dd9;
        }

        .btn-primary:disabled {
            background: #bfbfbf;
            cursor: not-allowed;
        }

        .btn-secondary {
            background: #f5f5f5;
            color: #595959;
            padding: 6px 12px;
            border: 1px solid #d9d9d9;
            border-radius: 4px;
            cursor: pointer;
            font-size: 13px;
            margin-left: 8px;
            line-height: 1.4;
            transition: all 0.3s;
        }

        .btn-secondary:hover {
            background: #e8e8e8;
        }

        .alert {
            padding: 8px 12px;
            margin-bottom: 12px;
            border-radius: 4px;
            font-size: 13px;
            line-height: 1.4;
        }

        .alert-success {
            background: #e6f4ea;
            color: #1e7e34;
            border: 1px solid #b7dfb9;
        }

        .alert-error {
            background: #fff2f0;
            color: #cf1322;
            border: 1px solid #ffccc7;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 12px;
            margin-top: 12px;
        }

        .stat-card {
            text-align: center;
            padding: 12px;
            border-radius: 4px;
        }

        .stat-value {
            font-size: 24px;
            font-weight: 600;
            margin-bottom: 4px;
        }

        .stat-label {
            color: #666;
            font-size: 12px;
        }

        .stat-success {
            background: #e6f4ea;
        }

        .stat-success .stat-value {
            color: #1e7e34;
        }

        .stat-error {
            background: #fff2f0;
        }

        .stat-error .stat-value {
            color: #cf1322;
        }

        .stat-total {
            background: #e1f0ff;
        }

        .stat-total .stat-value {
            color: #0056b3;
        }

        @media (max-width: 768px) {
            body {
                padding: 8px;
            }

            .container {
                width: 100%;
            }

            .form-row {
                grid-template-columns: 1fr;
            }

            .form-group input,
            .form-group select,
            .form-group textarea {
                font-size: 16px; /* 防止iOS缩放 */
            }

            .btn-secondary {
                margin-left: 0;
                margin-top: 8px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <a href="/api/v1/dashboard" class="back-button">← 返回仪表板</a>
            <h1>消息生产器</h1>
            <p>发送消息到指定Topic</p>
        </div>

        <div class="section">
            <div id="message" style="display: none;"></div>
            
            <form id="producerForm">
                <div class="form-row">
                    <div class="form-group">
                        <label for="topic">目标 Topic *</label>
                        <select id="topic" name="topic" required>
                            <option value="">选择Topic...</option>
                        </select>
                        <small>选择要发送消息的Topic</small>
                    </div>

                    <div class="form-group">
                        <label for="messageKey">消息Key</label>
                        <input type="text" id="messageKey" name="messageKey">
                        <small>用于分区路由的消息键（可选）</small>
                    </div>
                </div>

                <div class="form-group">
                    <label for="messageValue">消息内容 *</label>
                    <textarea id="messageValue" name="messageValue" rows="6" required placeholder="输入消息内容，支持JSON格式..."></textarea>
                    <small>消息的实际内容</small>
                </div>

                <div class="form-group">
                    <label for="headers">消息头（JSON格式）</label>
                    <textarea id="headers" name="headers" rows="3" placeholder='{"header1": "value1", "header2": "value2"}'></textarea>
                    <small>可选的消息头，JSON格式</small>
                </div>

                <div class="form-row">
                    <div class="form-group">
                        <label for="batchCount">批量发送数量</label>
                        <input type="number" id="batchCount" name="batchCount" min="1" max="1000" value="1">
                        <small>一次性发送的消息数量</small>
                    </div>

                    <div class="form-group">
                        <label for="interval">发送间隔（毫秒）</label>
                        <input type="number" id="interval" name="interval" min="0" value="0">
                        <small>批量发送时消息间的间隔</small>
                    </div>
                </div>

                <button type="submit" class="btn-primary" id="sendBtn">发送消息</button>
                <button type="button" class="btn-secondary" onclick="clearForm()">清空</button>
                <button type="button" class="btn-secondary" onclick="loadSampleMessage()">示例消息</button>
            </form>
        </div>

        <div class="section">
            <h3 style="margin-bottom: 12px; font-size: 15px; color: #1a1a1a;">发送统计</h3>
            <div class="stats-grid">
                <div class="stat-card stat-success">
                    <div class="stat-value" id="successCount">0</div>
                    <div class="stat-label">成功</div>
                </div>
                <div class="stat-card stat-error">
                    <div class="stat-value" id="errorCount">0</div>
                    <div class="stat-label">失败</div>
                </div>
                <div class="stat-card stat-total">
                    <div class="stat-value" id="totalCount">0</div>
                    <div class="stat-label">总计</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let stats = { success: 0, error: 0, total: 0 };

        // 页面加载时获取Topics列表
        document.addEventListener('DOMContentLoaded', async function() {
            await loadTopics();
        });

        // 加载Topics列表
        async function loadTopics() {
            try {
                const response = await fetch('/api/v1/clusters/dbmq-cluster/topics');
                const result = await response.json();
                
                if (result.success && result.data) {
                    const topicSelect = document.getElementById('topic');
                    topicSelect.innerHTML = '<option value="">选择Topic...</option>';
                    
                    result.data.forEach(topic => {
                        const option = document.createElement('option');
                        option.value = topic.name || topic.topicName;
                        option.textContent = topic.name || topic.topicName;
                        topicSelect.appendChild(option);
                    });
                } else {
                    showMessage('获取Topics列表失败: ' + (result.error || '未知错误'), 'error');
                }
            } catch (error) {
                showMessage('网络错误: ' + error.message, 'error');
            }
        }

        // 表单提交处理
        document.getElementById('producerForm').addEventListener('submit', async function(e) {
            e.preventDefault();
            await sendMessage();
        });

        // 发送消息
        async function sendMessage() {
            const formData = new FormData(document.getElementById('producerForm'));
            const sendBtn = document.getElementById('sendBtn');
            
            // 验证表单
            const topic = formData.get('topic');
            const messageValue = formData.get('messageValue');
            
            if (!topic || !messageValue) {
                showMessage('请填写必需的字段（Topic和消息内容）', 'error');
                return;
            }

            // 解析headers
            let headers = {};
            const headersStr = formData.get('headers');
            if (headersStr) {
                try {
                    headers = JSON.parse(headersStr);
                } catch (e) {
                    showMessage('消息头格式错误，请使用有效的JSON格式', 'error');
                    return;
                }
            }

            const batchCount = parseInt(formData.get('batchCount')) || 1;
            const interval = parseInt(formData.get('interval')) || 0;

            sendBtn.disabled = true;
            sendBtn.textContent = '发送中...';

            try {
                for (let i = 0; i < batchCount; i++) {
                    const messageData = {
                        topic: topic,
                        key: formData.get('messageKey') || null,
                        value: messageValue,
                        headers: headers
                    };

                    try {
                        const response = await fetch('/api/v1/clusters/dbmq-cluster/topics/' + encodeURIComponent(topic) + '/messages', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json'
                            },
                            body: JSON.stringify(messageData)
                        });

                        const result = await response.json();
                        
                        if (result.success) {
                            stats.success++;
                        } else {
                            stats.error++;
                            console.error('Message send failed:', result.error);
                        }
                    } catch (error) {
                        stats.error++;
                        console.error('Network error:', error);
                    }

                    stats.total++;
                    updateStats();

                    // 等待间隔时间
                    if (interval > 0 && i < batchCount - 1) {
                        await new Promise(resolve => setTimeout(resolve, interval));
                    }
                }

                showMessage('消息发送完成！成功: ' + stats.success + '，失败: ' + stats.error, 
                           stats.error === 0 ? 'success' : 'error');
                           
            } catch (error) {
                showMessage('发送失败: ' + error.message, 'error');
            } finally {
                sendBtn.disabled = false;
                sendBtn.textContent = '发送消息';
            }
        }

        // 更新统计显示
        function updateStats() {
            document.getElementById('successCount').textContent = stats.success;
            document.getElementById('errorCount').textContent = stats.error;
            document.getElementById('totalCount').textContent = stats.total;
        }

        // 清空表单
        function clearForm() {
            document.getElementById('producerForm').reset();
            stats = { success: 0, error: 0, total: 0 };
            updateStats();
        }

        // 加载示例消息
        function loadSampleMessage() {
            document.getElementById('messageKey').value = 'sample-key-' + Date.now();
            document.getElementById('messageValue').value = JSON.stringify({
                id: Date.now(),
                message: "这是一个示例消息",
                timestamp: new Date().toISOString(),
                data: {
                    user: "用户123",
                    action: "示例操作"
                }
            }, null, 2);
            document.getElementById('headers').value = JSON.stringify({
                "source": "dashboard",
                "version": "1.0"
            }, null, 2);
        }

        // 显示消息
        function showMessage(text, type) {
            const messageDiv = document.getElementById('message');
            messageDiv.className = 'alert alert-' + type;
            messageDiv.textContent = text;
            messageDiv.style.display = 'block';
            
            setTimeout(() => {
                messageDiv.style.display = 'none';
            }, 5000);
        }
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(html))
}
