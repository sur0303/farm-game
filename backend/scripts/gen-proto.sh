#!/usr/bin/env bash
# 生成所有 proto 的 Go 代码到 api/gen/。可重复执行(幂等)。
# 依赖:protoc、protoc-gen-go、protoc-gen-go-grpc(在 $HOME/go/bin)。
set -euo pipefail

cd "$(dirname "$0")/.."
export PATH="$PATH:$HOME/go/bin"

mkdir -p api/gen

# 收集所有 proto 文件
PROTOS=$(find api/proto -name '*.proto' | sort)

echo "generating go from:"
echo "$PROTOS" | sed 's/^/  /'

# --go_opt=module 让生成路径按 go_package 的 module 前缀裁剪落到 api/gen
# -I third_party 提供 google/protobuf/*.proto 等 well-known 类型(系统 protoc 未内置该路径)。
protoc -I=api/proto -I=third_party \
  --go_out=api/gen --go_opt=module=farmgame/api/gen \
  --go-grpc_out=api/gen --go-grpc_opt=module=farmgame/api/gen \
  $PROTOS

echo "done. generated:"
find api/gen -name '*.pb.go' | sort | sed 's/^/  /'
