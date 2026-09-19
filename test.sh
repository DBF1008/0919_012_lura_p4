#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

step() {
	printf '\n\033[1;34m==> [%s] %s\033[0m\n' "$1" "$2"
}

step 1/8 "go build ./..."
go build ./...

step 2/8 "go vet (config / proxy / transport/http/server)"
go vet ./config/... ./proxy/... ./transport/http/server/...

step 3/8 "config unit tests (incl. new config keys parsing)"
go test -count=1 -v ./config/...

step 4/8 "proxy drain middleware and factory draining tests"
go test -count=1 -v -run 'Drain|Draining' ./proxy/

step 5/8 "server graceful shutdown / health check unit tests"
go test -count=1 -v -run 'ConnTracker|HealthCheck|RunServer_health|gracefulShutdown' ./transport/http/server/

step 6/8 "server graceful shutdown / health check unit tests (race)"
go test -count=1 -race -run 'ConnTracker|HealthCheck|RunServer_health|gracefulShutdown' ./transport/http/server/

step 7/8 "proxy package full unit tests"
go test -count=1 ./proxy/

step 8/8 "server package full unit tests (incl. legacy TLS/h2c cases, needs network)"
go test -count=1 ./transport/http/server/

printf '\n\033[1;32mALL TESTS PASSED\033[0m\n'
