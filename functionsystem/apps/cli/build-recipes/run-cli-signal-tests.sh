#!/bin/bash
set -u
W=/data/work/yr-cli-verify
A=$W/audit-cli
export GOMAXPROCS=12 GO111MODULE=on GONOSUMDB="*" GOFLAGS="-mod=readonly"
export GOPROXY="https://goproxy.cn,direct" GOWORK=off CGO_ENABLED=0
cd "$W/functionsystem/apps/cli" || exit 9
go version > "$A/go-version.txt" 2>&1
go test -p 12 -count=1 -run "TestBlockedCommandWaitsForGracefulShutdown|AfterReady" ./utils/ -v > "$A/signal-tests.log" 2>&1
echo $? > "$A/signal-tests.rc"
go vet -p 12 ./utils/ ./internal/start/ > "$A/vet.log" 2>&1
echo $? > "$A/vet.rc"
echo CLI_TESTS_DONE
