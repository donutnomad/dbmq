package dbmq

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// Topic详情页面处理器
func (ras *RestAPIServer) topicDetailHandler(c *gin.Context) {
	topicName := c.Param("topicName")
	_ = topicName
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	c.String(http.StatusOK, html)
}

// 消费组详情页面处理器
func (ras *RestAPIServer) consumerGroupDetailHandler(c *gin.Context) {
	groupId := c.Param("groupId")
	_ = groupId
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	c.String(http.StatusOK, html)
}

// Topic创建页面处理器
func (ras *RestAPIServer) topicCreatePageHandler(c *gin.Context) {
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	c.String(http.StatusOK, html)
}

// Topic管理页面处理器
func (ras *RestAPIServer) topicManagePageHandler(c *gin.Context) {
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	c.String(http.StatusOK, html)
}

// 消息生产页面处理器
func (ras *RestAPIServer) messageProducerPageHandler(c *gin.Context) {
	html := ``

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.Status(http.StatusOK)
	c.String(http.StatusOK, html)
}
