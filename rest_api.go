package dbmq

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/donutnomad/dbmq/types"
	"github.com/gin-gonic/gin"
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
	engine        *gin.Engine
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

	// 创建 Gin 引擎
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(corsMiddleware())
	engine.Use(loggingMiddleware())

	return &RestAPIServer{
		config:        config,
		metricsClient: metricsClient,
		adminClient:   adminClient,
		engine:        engine,
	}, nil
}

// APIResponse 标准API响应格式
type APIResponse struct {
	Success bool   `json:"success"`           // 请求是否成功
	Data    any    `json:"data,omitempty"`    // 响应数据
	Error   string `json:"error,omitempty"`   // 错误信息
	Message string `json:"message,omitempty"` // 附加消息
}

// Start 启动REST API服务器
func (ras *RestAPIServer) Start() error {
	// 注册API路由
	ras.registerRoutes()

	// 创建HTTP服务器
	addr := fmt.Sprintf("%s:%d", ras.config.Host, ras.config.Port)
	ras.server = &http.Server{
		Addr:         addr,
		Handler:      ras.engine,
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
func (ras *RestAPIServer) registerRoutes() {
	api := ras.engine.Group(ras.config.Prefix)

	// 健康检查接口
	api.GET("/health", ras.healthHandler)
	api.GET("/actuator/health", ras.healthHandler) // 兼容Spring Boot
	api.GET("/actuator/info", ras.infoHandler)     // 兼容Spring Boot

	// 添加统计页面路由
	api.GET("/dashboard", ras.dashboardHandler)
	api.GET("/dashboard/data", ras.dashboardDataHandler)
	// 添加详情页面路由
	api.GET("/dashboard/topic/:topicName", ras.topicDetailHandler)
	api.GET("/dashboard/consumer-group/:groupId", ras.consumerGroupDetailHandler)
	// 添加Topic管理页面
	api.GET("/dashboard/topics/create", ras.topicCreatePageHandler)
	api.GET("/dashboard/topics/manage", ras.topicManagePageHandler)
	// 添加消息生产页面
	api.GET("/dashboard/producer", ras.messageProducerPageHandler)

	// 集群信息接口（兼容Kafka UI）
	api.GET("/clusters", ras.getClustersHandler)
	api.GET("/clusters/:clusterId/metrics", ras.getClusterMetricsHandler)
	api.GET("/clusters/:clusterId/brokers", ras.getBrokersHandler)

	// Topic管理接口（兼容Kafka UI）
	api.GET("/clusters/:clusterId/topics", ras.getTopicsHandler)
	api.POST("/clusters/:clusterId/topics", ras.createTopicHandler)
	api.GET("/clusters/:clusterId/topics/:topicName", ras.getTopicHandler)
	api.DELETE("/clusters/:clusterId/topics/:topicName", ras.deleteTopicHandler)
	api.GET("/clusters/:clusterId/topics/:topicName/metrics", ras.getTopicMetricsHandler)

	// 消费组管理接口（兼容Kafka UI）
	api.GET("/clusters/:clusterId/consumer-groups", ras.getConsumerGroupsHandler)
	api.GET("/clusters/:clusterId/consumer-groups/:groupId", ras.getConsumerGroupHandler)
	api.GET("/clusters/:clusterId/consumer-groups/:groupId/metrics", ras.getConsumerGroupMetricsHandler)

	// 兼容Kafka REST Proxy的接口
	api.GET("/topics", ras.listTopicsHandler)
	api.GET("/topics/:topicName", ras.getTopicInfoHandler)
	api.GET("/topics/:topicName/partitions", ras.getPartitionsHandler)
	api.GET("/brokers", ras.listBrokersHandler)

	// 扩展的DBMQ专用接口
	api.GET("/dbmq/stats", ras.getDBMQStatsHandler)
	api.GET("/dbmq/topics/:topicName/messages", ras.getTopicMessagesHandler)
}

// 健康检查处理器
func (ras *RestAPIServer) healthHandler(c *gin.Context) {
	// 检查数据库连接
	sqlDB, err := ras.config.DB.DB()
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Database connection error", err)
		return
	}

	if err := sqlDB.PingContext(c); err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Database ping failed", err)
		return
	}

	health := map[string]any{
		"status":    "UP",
		"timestamp": time.Now().Format(time.RFC3339),
		"components": map[string]any{
			"database": map[string]string{
				"status": "UP",
			},
		},
	}

	ras.writeSuccessResponse(c, health)
}

// 信息处理器
func (ras *RestAPIServer) infoHandler(c *gin.Context) {
	info := map[string]any{
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

	ras.writeSuccessResponse(c, info)
}

// 获取集群列表处理器
func (ras *RestAPIServer) getClustersHandler(c *gin.Context) {
	clusters := []map[string]any{
		{
			"clusterId":   "dbmq-cluster",
			"name":        "DBMQ Cluster",
			"brokerCount": 1,
			"status":      "online",
		},
	}

	ras.writeSuccessResponse(c, clusters)
}

// 获取集群指标处理器
func (ras *RestAPIServer) getClusterMetricsHandler(c *gin.Context) {
	metrics, err := ras.metricsClient.GetClusterMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get cluster metrics", err)
		return
	}

	ras.writeSuccessResponse(c, metrics)
}

// 获取Broker列表处理器
func (ras *RestAPIServer) getBrokersHandler(c *gin.Context) {
	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get broker metrics", err)
		return
	}

	brokers := []any{brokerMetrics}
	ras.writeSuccessResponse(c, brokers)
}

// 获取Topic列表处理器
func (ras *RestAPIServer) getTopicsHandler(c *gin.Context) {
	topics, err := ras.metricsClient.GetAllTopicsMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get topics", err)
		return
	}

	ras.writeSuccessResponse(c, topics)
}

// 创建Topic处理器
func (ras *RestAPIServer) createTopicHandler(c *gin.Context) {
	var req NewTopicRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		ras.writeErrorResponse(c, http.StatusBadRequest, "Invalid request body", err)
		return
	}

	err := ras.adminClient.CreateTopic(c, req)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to create topic", err)
		return
	}

	ras.writeSuccessResponse(c, map[string]string{
		"message": fmt.Sprintf("Topic %s created successfully", req.Name),
	})
}

// 获取单个Topic处理器
func (ras *RestAPIServer) getTopicHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	topic, err := ras.metricsClient.GetTopicMetrics(c, topicName)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(c, topic)
}

// 删除Topic处理器
func (ras *RestAPIServer) deleteTopicHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	err := ras.adminClient.DeleteTopics(c, []string{topicName})
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to delete topic", err)
		return
	}

	ras.writeSuccessResponse(c, map[string]string{
		"message": fmt.Sprintf("Topic %s deleted successfully", topicName),
	})
}

// 获取Topic指标处理器
func (ras *RestAPIServer) getTopicMetricsHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	metrics, err := ras.metricsClient.GetTopicMetrics(c, topicName)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(c, metrics)
}

// 获取消费组列表处理器
func (ras *RestAPIServer) getConsumerGroupsHandler(c *gin.Context) {
	groups, err := ras.metricsClient.GetAllConsumerGroupsMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get consumer groups", err)
		return
	}

	ras.writeSuccessResponse(c, groups)
}

// 获取单个消费组处理器
func (ras *RestAPIServer) getConsumerGroupHandler(c *gin.Context) {
	groupId := c.Param("groupId")

	group, err := ras.metricsClient.GetConsumerGroupMetrics(c, groupId)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Consumer group not found", err)
		return
	}

	ras.writeSuccessResponse(c, group)
}

// 获取消费组指标处理器
func (ras *RestAPIServer) getConsumerGroupMetricsHandler(c *gin.Context) {
	groupId := c.Param("groupId")

	metrics, err := ras.metricsClient.GetConsumerGroupMetrics(c, groupId)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Consumer group not found", err)
		return
	}

	ras.writeSuccessResponse(c, metrics)
}

// 兼容Kafka REST Proxy的Topic列表处理器
func (ras *RestAPIServer) listTopicsHandler(c *gin.Context) {
	topicNames, err := ras.adminClient.ListTopics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to list topics", err)
		return
	}

	ras.writeSuccessResponse(c, topicNames)
}

// 兼容Kafka REST Proxy的Topic信息处理器
func (ras *RestAPIServer) getTopicInfoHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	descriptions, err := ras.adminClient.DescribeTopics(c, []string{topicName})
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Topic not found", err)
		return
	}

	if desc, exists := descriptions[topicName]; exists {
		ras.writeSuccessResponse(c, desc)
	} else {
		ras.writeErrorResponse(c, http.StatusNotFound, "Topic not found", nil)
	}
}

// 获取分区信息处理器
func (ras *RestAPIServer) getPartitionsHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	topicMetrics, err := ras.metricsClient.GetTopicMetrics(c, topicName)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Topic not found", err)
		return
	}

	ras.writeSuccessResponse(c, topicMetrics.Partitions)
}

// 兼容Kafka REST Proxy的Broker列表处理器
func (ras *RestAPIServer) listBrokersHandler(c *gin.Context) {
	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get brokers", err)
		return
	}

	brokers := []any{
		map[string]any{
			"broker_id": brokerMetrics.BrokerID,
			"host":      brokerMetrics.Host,
			"port":      brokerMetrics.Port,
		},
	}

	ras.writeSuccessResponse(c, brokers)
}

// DBMQ统计信息处理器
func (ras *RestAPIServer) getDBMQStatsHandler(c *gin.Context) {
	clusterMetrics, err := ras.metricsClient.GetClusterMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get stats", err)
		return
	}

	brokerMetrics, err := ras.metricsClient.GetBrokerMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get broker stats", err)
		return
	}

	stats := map[string]any{
		"cluster": clusterMetrics,
		"broker":  brokerMetrics,
		"system": map[string]any{
			"uptime":    brokerMetrics.Uptime,
			"version":   brokerMetrics.Version,
			"timestamp": time.Now().Format(time.RFC3339),
		},
	}

	ras.writeSuccessResponse(c, stats)
}

// 获取Topic消息处理器
func (ras *RestAPIServer) getTopicMessagesHandler(c *gin.Context) {
	topicName := c.Param("topicName")

	// 解析查询参数
	partitionStr := c.Query("partition")
	offsetStr := c.Query("offset")
	limitStr := c.Query("limit")
	searchKey := c.Query("search")
	fromTime := c.Query("from_time")
	toTime := c.Query("to_time")

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

	messages, err := ras.getMessages(c, topicName, partitionInt, offsetInt, limitInt, searchKey, fromTime, toTime)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get messages", err)
		return
	}

	result := map[string]any{
		"topic":     topicName,
		"partition": partitionStr,
		"offset":    offsetInt,
		"limit":     limitInt,
		"search":    searchKey,
		"messages":  messages,
		"total":     len(messages),
	}

	ras.writeSuccessResponse(c, result)
}

// getMessages 获取消息列表
func (ras *RestAPIServer) getMessages(ctx context.Context, topicName string, partition *uint, offset int64, limit int, searchKey, fromTime, toTime string) ([]map[string]any, error) {
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
	result := make([]map[string]any, len(messages))
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

		result[i] = map[string]any{
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
func (ras *RestAPIServer) writeSuccessResponse(c *gin.Context, data any) {
	c.JSON(http.StatusOK, APIResponse{
		Success: true,
		Data:    data,
	})
}

// 写入错误响应
func (ras *RestAPIServer) writeErrorResponse(c *gin.Context, statusCode int, message string, err error) {
	response := APIResponse{
		Success: false,
		Message: message,
	}
	if err != nil {
		response.Error = err.Error()
	}
	c.JSON(statusCode, response)
}

// CORS中间件
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

// 日志中间件
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

// 仪表板数据处理器
func (ras *RestAPIServer) dashboardDataHandler(c *gin.Context) {
	// 获取 Topics 数据
	topics, err := ras.metricsClient.GetAllTopicsMetrics(c)
	if err != nil {
		topics = []TopicMetrics{} // 如果出错，返回空数组而不是失败
	}

	// 获取消费组数据
	consumerGroups, err := ras.metricsClient.GetAllConsumerGroupsMetrics(c)
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

	ras.writeSuccessResponse(c, dashboardData)
}

// 统计仪表板页面处理器
func (ras *RestAPIServer) dashboardHandler(c *gin.Context) {
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, html)
}
