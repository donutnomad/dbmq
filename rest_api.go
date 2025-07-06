package dbmq

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"time"

	"github.com/donutnomad/dbmq/types"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

//go:embed dashboard-ui/out/*
var embedFS embed.FS

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
	metricsClient, err := NewMetricsClient(config.DB)
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
	// 例如：访问 /static/index.html 而不是 /static/web/index.html
	staticFiles, err := fs.Sub(embedFS, "dashboard-ui/out")
	if err != nil {
		panic(err)
	}
	ras.engine.StaticFS("/static", http.FS(staticFiles))

	api := ras.engine.Group(ras.config.Prefix)

	// 健康检查接口
	api.GET("/health", ras.healthHandler)
	api.GET("/actuator/health", ras.healthHandler) // 兼容Spring Boot
	api.GET("/actuator/info", ras.infoHandler)     // 兼容Spring Boot

	// 添加统计页面路由
	api.GET("/dashboard", ras.dashboardHandler)
	api.GET("/dashboard/data", ras.dashboardDataHandler)

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

	// 兼容Kafka REST Proxy的接口
	api.GET("/topics", ras.listTopicsHandler)
	api.GET("/topics/:topicName", ras.getTopicInfoHandler)
	api.GET("/topics/:topicName/partitions", ras.getPartitionsHandler)
	api.GET("/brokers", ras.listBrokersHandler)

	// 扩展的DBMQ专用接口
	api.GET("/dbmq/stats", ras.getDBMQStatsHandler)
	api.GET("/dbmq/topics/:topicName/messages", ras.getTopicMessagesHandler)
	api.GET("/dbmq/topics/:topicName/partitions/:partitionId/stats", ras.getPartitionStatsHandler)

	// 新增：扩展的消费组详情API
	api.GET("/dbmq/consumer-groups/:groupId/extended", ras.getConsumerGroupExtendedHandler)
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
	// 检查是否需要详细的分区信息
	includePartitionStats := c.Query("includePartitionStats") == "true"

	topics, err := ras.metricsClient.GetAllTopicsMetrics(c)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get topics", err)
		return
	}

	// 如果需要分区统计信息，则为每个topic获取详细信息
	if includePartitionStats {
		enhancedTopics := make([]map[string]any, len(topics))
		for i, topic := range topics {
			topicMap := map[string]any{
				"name":           topic.TopicName,
				"partitionCount": topic.PartitionCount,
				"messageCount":   topic.MessageCount,
				"sizeBytes":      topic.SizeBytes,
				"createdAt":      topic.CreatedAt,
			}

			// 获取每个分区的统计信息
			if topic.PartitionCount > 0 {
				partitionStats := make([]map[string]any, topic.PartitionCount)
				for p := uint(0); p < uint(topic.PartitionCount); p++ {
					stats, err := ras.getPartitionStats(c, topic.TopicName, p)
					if err != nil {
						// 如果获取分区统计失败，设置默认值
						partitionStats[p] = map[string]any{
							"partition":      p,
							"firstMessageId": -1,
							"lastMessageId":  -1,
							"messageCount":   0,
							"sizeBytes":      0,
						}
					} else {
						partitionStats[p] = map[string]any{
							"partition":      stats.Partition,
							"firstMessageId": stats.FirstMessageID,
							"lastMessageId":  stats.LastMessageID,
							"messageCount":   stats.MessageCount,
							"sizeBytes":      stats.SizeBytes,
							"createdAt":      stats.CreatedAt,
							"updatedAt":      stats.UpdatedAt,
						}
					}
				}
				topicMap["partitionStats"] = partitionStats
			}

			enhancedTopics[i] = topicMap
		}
		ras.writeSuccessResponse(c, enhancedTopics)
	} else {
		ras.writeSuccessResponse(c, topics)
	}
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

// 获取消费组扩展信息处理器
func (ras *RestAPIServer) getConsumerGroupExtendedHandler(c *gin.Context) {
	groupId := c.Param("groupId")

	// 获取基本消费组信息
	group, err := ras.metricsClient.GetConsumerGroupMetrics(c, groupId)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusNotFound, "Consumer group not found", err)
		return
	}

	// 查询消费组代际信息
	var generationInfo struct {
		GenerationID int       `json:"generationId"`
		LeaderID     string    `json:"leaderId"`
		UpdatedAt    time.Time `json:"updatedAt"`
	}
	err = ras.config.DB.Table("mq_consumer_group_generations").
		Select("generation_id, leader_id, updated_at").
		Where("group_id = ?", groupId).
		First(&generationInfo).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get generation info", err)
		return
	}

	// 查询消费组成员详细信息（包括离线消费者）
	var members []struct {
		ConsumerID         string     `json:"consumerId"`
		GenerationID       int        `json:"generationId"`
		SubscribedTopics   string     `json:"subscribedTopics"`
		AssignedPartitions string     `json:"assignedPartitions"`
		Offline            bool       `json:"offline"`
		LastHeartbeat      time.Time  `json:"lastHeartbeat"`
		OfflineAt          *time.Time `json:"offlineAt"`
	}
	err = ras.config.DB.Table("mq_consumer_heartbeats").
		Select("consumer_id, generation_id, subscribed_topics, assigned_partitions, offline, last_heartbeat, offline_at").
		Where("group_id = ?", groupId).
		Order("offline ASC, last_heartbeat DESC"). // 在线的排在前面，然后按最后心跳时间排序
		Find(&members).Error
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get member info", err)
		return
	}

	// 查询消费组偏移量提交信息
	var offsets []struct {
		Topic                 string        `json:"topic"`
		Partition             int           `json:"partition"`
		CommittedOffset       int64         `json:"committedOffset"`
		GenerationID          int           `json:"generationId"`
		Metadata              string        `json:"metadata"`
		UpdatedAt             time.Time     `json:"updatedAt"`
		InitialTopicWatermark sql.NullInt64 `json:"initialTopicWatermark"`
	}
	err = ras.config.DB.Table("mq_consumer_group_offsets").
		Select("topic, `partition`, committed_offset, generation_id, metadata, updated_at, initial_topic_watermark").
		Where("group_id = ?", groupId).
		Find(&offsets).Error
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get offset info", err)
		return
	}

	// 处理成员信息（包括所有历史消费者）
	enhancedMembers := make([]map[string]any, 0, len(members))

	// 首先处理所有数据库中的消费者记录
	for _, detail := range members {
		memberMap := make(map[string]any)
		memberMap["memberId"] = detail.ConsumerID
		memberMap["clientId"] = detail.ConsumerID // 使用ConsumerID作为ClientID
		memberMap["host"] = "unknown"             // 暂时设为unknown，后续可扩展
		memberMap["generationId"] = detail.GenerationID
		memberMap["offline"] = detail.Offline
		memberMap["lastHeartbeat"] = detail.LastHeartbeat.Format(time.RFC3339)

		if detail.OfflineAt != nil {
			memberMap["offlineAt"] = detail.OfflineAt.Format(time.RFC3339)
		}

		// 解析订阅的Topic
		var subscribedTopics []string
		if detail.SubscribedTopics != "" {
			if err := json.Unmarshal([]byte(detail.SubscribedTopics), &subscribedTopics); err == nil {
				memberMap["subscribedTopics"] = subscribedTopics
			}
		}

		// 解析分区分配
		memberMap["assignment"] = map[string]any{}
		if detail.AssignedPartitions != "" {
			// 先解析为数组格式
			var partitionInfoArray []map[string]any
			if err := json.Unmarshal([]byte(detail.AssignedPartitions), &partitionInfoArray); err == nil {
				// 转换为按topic分组的map格式
				assignmentMap := make(map[string][]int)
				for _, partitionInfo := range partitionInfoArray {
					if topic, ok := partitionInfo["Topic"].(string); ok {
						if partition, ok := partitionInfo["Partition"].(float64); ok {
							assignmentMap[topic] = append(assignmentMap[topic], int(partition))
						}
					}
				}
				if len(assignmentMap) > 0 {
					memberMap["assignment"] = assignmentMap
				}
			}
		}

		// 判断消费者状态
		heartbeatTimeout := 30 * time.Second // 可以从配置中获取
		isTimeout := time.Since(detail.LastHeartbeat) > heartbeatTimeout

		if detail.Offline {
			memberMap["status"] = "offline"
		} else if isTimeout {
			memberMap["status"] = "timeout"
		} else {
			memberMap["status"] = "online"
		}

		enhancedMembers = append(enhancedMembers, memberMap)
	}

	// 处理分区延迟信息，增强分区级别的消费信息
	enhancedLags := make([]map[string]any, 0, len(group.PartitionLags))
	for _, lag := range group.PartitionLags {
		// 获取分区统计信息
		partitionStats, err := ras.getPartitionStats(c, lag.Topic, uint(lag.Partition))

		lagMap := map[string]any{
			"topic":         lag.Topic,
			"partition":     lag.Partition,
			"currentOffset": lag.CurrentOffset,
			"latestOffset":  lag.LatestOffset,
			"lag":           lag.Lag,
		}

		// 添加分区级别的详细信息
		if err == nil && partitionStats != nil {
			lagMap["firstMessageId"] = partitionStats.FirstMessageID
			lagMap["lastMessageId"] = partitionStats.LastMessageID
			lagMap["totalMessageCount"] = partitionStats.MessageCount
			lagMap["partitionSizeBytes"] = partitionStats.SizeBytes

			// 计算消费进度
			if partitionStats.MessageCount > 0 && lag.CurrentOffset >= partitionStats.FirstMessageID {
				// 计算已消费消息数量（基于ID范围）
				consumedMessages := lag.CurrentOffset - partitionStats.FirstMessageID + 1
				if consumedMessages > partitionStats.MessageCount {
					consumedMessages = partitionStats.MessageCount
				}

				// 计算消费进度百分比
				consumedPercentage := float64(consumedMessages) / float64(partitionStats.MessageCount) * 100

				lagMap["consumedMessages"] = consumedMessages
				lagMap["remainingMessages"] = partitionStats.MessageCount - consumedMessages
				lagMap["consumedPercentage"] = consumedPercentage
			} else {
				// 如果还没开始消费或数据异常
				lagMap["consumedMessages"] = 0
				lagMap["remainingMessages"] = partitionStats.MessageCount
				lagMap["consumedPercentage"] = 0.0
			}
		} else {
			// 如果无法获取分区统计信息，设置默认值
			lagMap["firstMessageId"] = -1
			lagMap["lastMessageId"] = -1
			lagMap["totalMessageCount"] = 0
			lagMap["partitionSizeBytes"] = 0
			lagMap["consumedMessages"] = 0
			lagMap["remainingMessages"] = 0
			lagMap["consumedPercentage"] = 0.0
		}

		// 查找匹配的偏移量提交信息
		for _, offset := range offsets {
			if offset.Topic == lag.Topic && offset.Partition == lag.Partition {
				lagMap["metadata"] = offset.Metadata
				lagMap["updatedAt"] = offset.UpdatedAt.Format(time.RFC3339)
				lagMap["generationId"] = offset.GenerationID
				// 添加初始水位线信息
				if offset.InitialTopicWatermark.Valid {
					lagMap["initialTopicWatermark"] = offset.InitialTopicWatermark.Int64
				} else {
					lagMap["initialTopicWatermark"] = nil
				}
				break
			}
		}
		enhancedLags = append(enhancedLags, lagMap)
	}

	// 确定消费模式
	commitMode := "unknown"
	if len(offsets) > 0 {
		// 检查最近提交的偏移量时间间隔
		var commitIntervals []time.Duration
		for i := 1; i < len(offsets); i++ {
			if offsets[i].Topic == offsets[i-1].Topic && offsets[i].Partition == offsets[i-1].Partition {
				interval := offsets[i].UpdatedAt.Sub(offsets[i-1].UpdatedAt)
				commitIntervals = append(commitIntervals, interval)
			}
		}

		// 如果存在多个提交记录，根据提交间隔判断模式
		if len(commitIntervals) > 0 {
			// 计算平均提交间隔
			var totalInterval time.Duration
			for _, interval := range commitIntervals {
				totalInterval += interval
			}
			avgInterval := totalInterval / time.Duration(len(commitIntervals))

			// 如果平均间隔在5-15秒范围内，可能是自动提交
			if avgInterval >= 5*time.Second && avgInterval <= 15*time.Second {
				commitMode = "auto"
			} else {
				commitMode = "manual"
			}
		} else {
			// 默认假设为手动提交
			commitMode = "manual"
		}
	}

	// 构建扩展信息
	extendedInfo := map[string]any{
		"members":            enhancedMembers,
		"partitionLags":      enhancedLags,
		"generationId":       generationInfo.GenerationID,
		"lastActivity":       generationInfo.UpdatedAt.Format(time.RFC3339),
		"coordinator":        "coordinator",
		"commitMode":         commitMode,
		"assignmentStrategy": "range", // 默认分配策略
	}

	ras.writeSuccessResponse(c, extendedInfo)
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

	messages, total, err := ras.getMessages(c, topicName, partitionInt, offsetInt, limitInt, searchKey, fromTime, toTime)
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
		"total":     total,
	}

	ras.writeSuccessResponse(c, result)
}

// getMessages 获取消息列表
func (ras *RestAPIServer) getMessages(ctx context.Context, topicName string, partition *uint, offset int64, limit int, searchKey, fromTime, toTime string) ([]map[string]any, int64, error) {
	var messages []types.Message
	query := ras.config.DB.WithContext(ctx).Where("topic = ?", topicName)

	// 分区过滤
	if partition != nil {
		query = query.Where("`partition` = ?", *partition)
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

	var total int64
	query.Model(&types.Message{}).Count(&total)

	// 排序和限制
	err := query.Order("created_at DESC").Offset(int(offset)).Limit(limit).Find(&messages).Error
	if err != nil {
		return nil, 0, err
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
			"offset":    msg.ID,
			"key":       messageKey,
			"value":     messageValue,
			"timestamp": msg.CreatedAt.Format(time.RFC3339),
			"size":      len(msg.Body),
			"headers":   msg.Headers,
		}
	}

	return result, total, nil
}

// PartitionStats 分区统计信息
type PartitionStats struct {
	Topic          string `json:"topic"`
	Partition      uint   `json:"partition"`
	FirstMessageID int64  `json:"firstMessageId"`
	LastMessageID  int64  `json:"lastMessageId"`
	MessageCount   int64  `json:"messageCount"`
	SizeBytes      int64  `json:"sizeBytes"`
	CreatedAt      string `json:"createdAt,omitempty"`
	UpdatedAt      string `json:"updatedAt,omitempty"`
}

// 获取分区统计信息处理器
func (ras *RestAPIServer) getPartitionStatsHandler(c *gin.Context) {
	topicName := c.Param("topicName")
	partitionIdStr := c.Param("partitionId")

	partitionId, err := strconv.ParseUint(partitionIdStr, 10, 32)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusBadRequest, "Invalid partition ID", err)
		return
	}

	partition := uint(partitionId)

	// 获取分区统计信息
	stats, err := ras.getPartitionStats(c, topicName, partition)
	if err != nil {
		ras.writeErrorResponse(c, http.StatusInternalServerError, "Failed to get partition stats", err)
		return
	}

	ras.writeSuccessResponse(c, stats)
}

// getPartitionStats 获取分区统计信息
func (ras *RestAPIServer) getPartitionStats(ctx context.Context, topicName string, partition uint) (*PartitionStats, error) {
	var stats struct {
		FirstMessageID int64  `gorm:"column:first_message_id"`
		LastMessageID  int64  `gorm:"column:last_message_id"`
		MessageCount   int64  `gorm:"column:message_count"`
		SizeBytes      int64  `gorm:"column:size_bytes"`
		CreatedAt      string `gorm:"column:created_at"`
		UpdatedAt      string `gorm:"column:updated_at"`
	}

	// 使用一个查询获取所有统计信息
	sql := `
		SELECT 
			COALESCE(MIN(id), -1) AS first_message_id,
			COALESCE(MAX(id), -1) AS last_message_id,
			COUNT(*) AS message_count,
			COALESCE(SUM(LENGTH(body)), 0) AS size_bytes,
			COALESCE(MIN(created_at), '') AS created_at,
			COALESCE(MAX(created_at), '') AS updated_at
		FROM mq_messages 
		WHERE topic = ? AND ` + "`partition`" + ` = ?`

	err := ras.config.DB.WithContext(ctx).Raw(sql, topicName, partition).Scan(&stats).Error
	if err != nil {
		return nil, err
	}

	return &PartitionStats{
		Topic:          topicName,
		Partition:      partition,
		FirstMessageID: stats.FirstMessageID,
		LastMessageID:  stats.LastMessageID,
		MessageCount:   stats.MessageCount,
		SizeBytes:      stats.SizeBytes,
		CreatedAt:      stats.CreatedAt,
		UpdatedAt:      stats.UpdatedAt,
	}, nil
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
	systemInfo := map[string]any{
		"uptime":  uptime,
		"version": "1.0.0",
	}

	dashboardData := map[string]any{
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
