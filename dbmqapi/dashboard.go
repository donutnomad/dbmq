package dbmqapi

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// DashboardHandler 返回一个 gin.HandlerFunc，用于服务嵌入的 Dashboard 静态文件。
// prefix 是挂载路径，例如 "/dashboard"。
//
// 用法:
//
//	r := gin.Default()
//	r.GET("/dashboard/*filepath", dbmqapi.DashboardHandler("/dashboard"))
func DashboardHandler(prefix string) gin.HandlerFunc {
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
