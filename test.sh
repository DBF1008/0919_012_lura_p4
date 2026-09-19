#!/usr/bin/env bash
#
# test.sh - 手动运行本次优雅停机改动的全部单元测试
#
# 涵盖:
#   1. config          - GracefulShutdownTimeout / HealthCheckPath 配置解析
#   2. proxy           - defaultFactory 排空(draining)感知与 ErrServiceDraining
#   3. transport/http/server - 连接追踪器、/health 与 /ready 端点、优雅停机流程
#
# 注意: 部分测试会监听本地端口并发起 localhost 请求, 请在网络不受限的环境运行。

set -euo pipefail
cd "$(dirname "$0")"

echo "==> 1/6 编译整个项目"
go build ./...

echo "==> 2/6 go vet 改动涉及的包"
go vet ./config/... ./proxy/... ./transport/http/server/...

echo "==> 3/6 config: 新增配置项解析测试"
go test -count=1 -v -run "TestNewParser" ./config/

echo "==> 4/6 proxy: 排空状态与 defaultFactory 测试"
go test -count=1 -v -run "TestNewDrainMiddleware|TestDefaultFactory" ./proxy/

echo "==> 5/6 transport/http/server: 连接追踪器 / 健康端点 / 优雅停机测试"
go test -count=1 -v -timeout 120s \
    -run "TestConnectionTracker|TestHealthHandler|TestRunServer_HealthAndReadyEndpoints|TestRunServer_GracefulShutdown" \
    ./transport/http/server/

echo "==> 6/6 三个改动包的全量单元测试"
go test -count=1 -timeout 300s ./config/... ./proxy/... ./transport/http/server/...

echo
echo "全部测试通过 ✅"
