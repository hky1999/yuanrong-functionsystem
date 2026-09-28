#!/bin/bash
# yr CLI build attempt 2: generate the pb package first (protoc + go plugins,
# versions pinned to the repo's build.sh), then build ./cmd/yr.
set -u
W=/data/work/yr-cli-verify
A=$W/audit-cli
export GOMAXPROCS=12 GO111MODULE=on GONOSUMDB="*" GOFLAGS="-mod=readonly"
export GOPROXY="https://goproxy.cn,direct" GOWORK=off CGO_ENABLED=0
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 > "$A/pb-tools.log" 2>&1 || exit 11
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 >> "$A/pb-tools.log" 2>&1 || exit 12
export PATH="$(go env GOPATH)/bin:$PATH"
sha256sum "$(command -v protoc-gen-go)" "$(command -v protoc-gen-go-grpc)" "$(command -v protoc)" > "$A/toolchain-sha.txt"
cd "$W/functionsystem/apps/cli" || exit 9
sha256sum proto/message.proto > "$A/proto-input.sha256"
mkdir -p internal/pb
protoc --proto_path=proto --go_out=. --go_opt=module=cli \
  --go-grpc_out=. --go-grpc_opt=module=cli proto/message.proto >> "$A/pb-gen.log" 2>&1 || exit 13
find internal/pb -name '*.go' | LC_ALL=C sort | xargs sha256sum > "$A/generated-pb.sha256"
mkdir -p "$W/out-cli"
go build -p 12 -trimpath -o "$W/out-cli/yr" ./cmd/ > "$A/cli-build2.log" 2>&1
rc=$?
echo "$rc" > "$A/cli-build2.rc"
if [ "$rc" -eq 0 ]; then
  sha256sum "$W/out-cli/yr" > "$A/cli-yr.sha256"
  stat -c "%s bytes" "$W/out-cli/yr" >> "$A/cli-yr.sha256"
fi
echo BUILD2_DONE
exit "$rc"
