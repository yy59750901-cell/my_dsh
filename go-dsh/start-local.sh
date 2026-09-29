#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="$PROJECT_DIR/config.local.json"

if ! command -v go >/dev/null 2>&1; then
  printf '找不到 Go，请先安装 Go 1.25 或更高版本。\n' >&2
  exit 1
fi
if [[ ! -f "$CONFIG_FILE" ]]; then
  printf '找不到本地配置文件：%s\n' "$CONFIG_FILE" >&2
  exit 1
fi

# 运行整个 cmd/dsh-server 包，而不是单独运行 main.go；密钥只从本地私有配置读取。
# 支持 ./start-local.sh -check-config：仅校验配置，不启动服务或请求远端模型。
exec go -C "$PROJECT_DIR" run -mod=readonly ./cmd/dsh-server -config "$CONFIG_FILE" "$@"
