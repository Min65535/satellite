#!/usr/bin/env bash

# 遇到命令失败、未定义变量或管道错误时立即退出。
set -euo pipefail

# 始终以脚本所在目录作为项目根目录，避免从其他目录执行时找不到源码或配置。
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="${ROOT_DIR}/satellite_server"
CONFIG_PATH="${1:-${CONFIG_PATH:-${ROOT_DIR}/config.yaml}}"

# 第一个位置参数是配置文件路径，其余参数会继续传给服务端程序。
if (( $# > 0 )); then
    shift
fi

if [[ ! -f "${CONFIG_PATH}" ]]; then
    echo "配置文件不存在: ${CONFIG_PATH}" >&2
    exit 1
fi

cd "${ROOT_DIR}"

echo "正在编译卫星服务端..."
go build -o "${BINARY_PATH}" ./cmd/server

echo "正在启动卫星服务端，配置文件: ${CONFIG_PATH}"
exec "${BINARY_PATH}" -config "${CONFIG_PATH}" "$@"
