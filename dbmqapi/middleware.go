package dbmqapi

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// accessTokenMiddleware 校验访问令牌。
// 支持三种方式传递 token：
//   - Query 参数: ?token=xxx
//   - Header: Authorization: Bearer xxx
//   - Cookie: access_token=xxx
func accessTokenMiddleware(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1. Query 参数
		if c.Query("token") == token {
			c.Next()
			return
		}
		// 2. Authorization Header
		if auth := c.GetHeader("Authorization"); strings.HasPrefix(auth, "Bearer ") && auth[7:] == token {
			c.Next()
			return
		}
		// 3. Cookie
		if cookie, err := c.Cookie("access_token"); err == nil && cookie == token {
			c.Next()
			return
		}
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{
			"success": false,
			"error":   "unauthorized: invalid or missing access token",
		})
	}
}

// corsMiddleware CORS 中间件
func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")
		if origin == "" {
			origin = "*"
		}

		// 设置 CORS 响应头
		c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers")

		// 开发环境使用较短的缓存时间，生产环境可以设置为 86400（24小时）
		// 开发时如果遇到 CORS 缓存问题，设置为 0 可以禁用预检缓存
		c.Writer.Header().Set("Access-Control-Max-Age", "600") // 10分钟

		// 处理 OPTIONS 预检请求
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
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
		slog.Info("request",
			"time", start.Format("2006-01-02 15:04:05"),
			"method", c.Request.Method,
			"path", c.Request.URL.Path,
			"status", c.Writer.Status(),
			"duration", duration,
		)
	}
}
