package dbmqapi

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/donutnomad/dbmq/internal/interfaces"
	"github.com/gin-gonic/gin"
)

func RegisterAPIs(routes gin.IRouter, dashboardPath, apiPath, accessToken string, db interfaces.DB, preHandlers ...gin.HandlerFunc) {
	registerAPIs(routes, dashboardPath, apiPath, accessToken, newDeps(db), preHandlers...)
}

type name interface {
	BasePath() string
}

type wrapperBasePath struct {
	basePath string
	gin.IRouter
}

func (w *wrapperBasePath) BasePath() string {
	return w.basePath
}

func registerAPIs(routes gin.IRouter, dashboardPath, apiPath, accessToken string, deps *Deps, preHandlers ...gin.HandlerFunc) {
	if accessToken != "" {
		preHandlers = append(preHandlers, accessTokenMiddleware(accessToken))
	}

	// Dashboard UI（不需要 token，认证由前端 AuthGuard 通过 API 请求判断）
	dashPath := dashboardPath
	if dashPath == "" {
		panic("dashPath is empty")
	}
	routes.GET(dashPath+"/*filepath", dashboardHandler(dashPath))

	// 替换掉默认前缀: "/dbmq/api/v1/"
	group := &wrapperBasePath{"/dbmq/api/v1/", routes.Group(apiPath)}

	h := &implHandlers{preHandlers}
	NewHealthAPIWrap(NewHealthAPI(deps), h).BindAll(group)
	NewDashboardAPIWrap(NewDashboardAPI(deps), h).BindAll(group)
	NewTopicAPIWrap(NewTopicAPI(deps), h).BindAll(group)
	NewConsumerAPIWrap(NewConsumerAPI(deps), h).BindAll(group)
	NewConsumerGroupAPIWrap(NewConsumerGroupAPI(deps), h).BindAll(group)
	NewDBMQAPIWrap(NewDBMQAPI(deps), h).BindAll(group)
	NewClusterAPIWrap(NewClusterAPI(deps), h).BindAll(group)
	NewManualAssignmentAPIWrap(NewManualAssignmentAPI(deps), h).BindAll(group)
}

// dashboardHandler 返回一个 gin.HandlerFunc，用于服务嵌入的 Dashboard 静态文件。
func dashboardHandler(prefix string) gin.HandlerFunc {
	sub, err := fs.Sub(staticFS, "static/dashboard")
	if err != nil {
		panic("dbmqapi: static/dashboard not embedded, run build-single-html.sh first")
	}
	fileServer := http.StripPrefix(prefix, http.FileServer(http.FS(sub)))
	return func(c *gin.Context) {
		// 去掉前缀后的路径
		path := strings.TrimPrefix(c.Request.URL.Path, prefix)
		path = strings.TrimPrefix(path, "/")
		// 尝试打开文件，如果不存在则回退到 index.html（SPA 路由支持）
		if path != "" {
			if _, err := fs.Stat(sub, path); err != nil {
				// 尝试 path.html (Next.js 静态导出格式)
				if _, err := fs.Stat(sub, path+".html"); err == nil {
					c.Request.URL.Path = prefix + "/" + path + ".html"
				} else {
					// 回退到 index.html
					c.Request.URL.Path = prefix + "/index.html"
				}
			}
		}
		fileServer.ServeHTTP(c.Writer, c.Request)
	}
}

type implHandlers struct {
	handlers []gin.HandlerFunc
}

func (h *implHandlers) PreHandlers() []gin.HandlerFunc {
	return h.handlers
}
