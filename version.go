package dbmq

import "runtime/debug"

const modulePath = "github.com/donutnomad/dbmq"

// Version 返回模块版本号。
// 当 dbmq 是 main module 时，从 info.Main 读取；
// 当 dbmq 作为库依赖时，从 info.Deps 中查找自身模块路径。
// 本地开发时 fallback 到 "dev"。
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}

	// 作为 main module 直接运行
	if info.Main.Path == modulePath {
		if v := info.Main.Version; v != "" && v != "(devel)" {
			return v
		}
		return "dev"
	}

	// 作为库依赖时，从 Deps 中查找
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			if dep.Replace != nil {
				return "dev"
			}
			return dep.Version
		}
	}

	return "dev"
}
