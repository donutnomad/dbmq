// Package main 运行DBMQ超级演示
package main

import (
	"fmt"
	"github.com/donutnomad/dbmq/examples"
	"os"
	"strings"
)

func main() {
	fmt.Println("🎯 DBMQ 超级演示程序")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Println()

	// 检查是否有命令行参数
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "admin":
			fmt.Println("🔧 运行管理客户端演示...")
			examples.AdminExample()
		case "batch":
			fmt.Println("📦 运行批量拉取演示...")
			// 这里可以添加批量拉取演示的调用
			fmt.Println("批量拉取演示尚未实现")
		case "super":
			fmt.Println("🚀 运行超级演示...")
			examples.SuperDemo()
		default:
			printUsage()
		}
	} else {
		// 默认运行超级演示
		examples.SuperDemo()
	}
}

func printUsage() {
	fmt.Println("用法:")
	fmt.Println("  go run cmd/super_demo/main.go [demo_type]")
	fmt.Println()
	fmt.Println("演示类型:")
	fmt.Println("  super  - 运行超级演示（默认）")
	fmt.Println("  admin  - 运行管理客户端演示")
	fmt.Println("  batch  - 运行批量拉取演示")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  go run cmd/super_demo/main.go super")
	fmt.Println("  go run cmd/super_demo/main.go admin")
}
