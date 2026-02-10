


super:
	go run examples/super_demo/main.go

rmq:
	cd dashboard-ui && npm run build
	go run examples/rmq_demo/main.go

# 集成演示 - 完整的功能演示,包含自动化生产者、消费者和Web UI
demo:
	cd examples/integration_demo && ./start.sh

demo-build:
	cd examples/integration_demo && go build -o dbmq_demo

demo-run:
	cd examples/integration_demo && go run main.go

# 启动 Web Dashboard (在另一个终端运行)
demo-ui:
	cd examples/integration_demo && ./start_dashboard.sh
