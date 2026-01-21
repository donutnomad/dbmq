package dbmqapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// onGinBind 绑定请求参数
func onGinBind(c *gin.Context, val any, typ string) bool {
	var err error
	switch typ {
	case "JSON":
		err = c.ShouldBindJSON(val)
	case "FORM":
		err = c.ShouldBind(val)
	case "QUERY":
		err = c.ShouldBindQuery(val)
	default:
		err = c.ShouldBind(val)
	}
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return false
	}
	return true
}

// onGinResponse 响应处理
func onGinResponse[T any](c *gin.Context, data T, err error) {
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, data)
}

// onGinBindErr 绑定错误处理
func onGinBindErr(c *gin.Context, err error) {
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}
