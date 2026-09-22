#!/usr/bin/env bash

# 遇到命令失败、未定义变量或管道错误时立即退出。
set -euo pipefail

# 参数模板：
#   $1 服务端 UDP 地址，默认 127.0.0.1:9000
#   $2 设备 ID，默认 10001
#   $3 测试图片路径，可选；为空时客户端自动生成模拟图片
#   $4... 其他需要继续传给客户端的参数
#
# 使用示例：
#   ./serve_client.sh
#   ./serve_client.sh 127.0.0.1:9000 10001
#   ./serve_client.sh 192.168.1.10:9000 10002 /data/test.jpg
#
# 也可以通过环境变量提供默认值：
#   SERVER_ADDRESS=192.168.1.10:9000 DEVICE_ID=10003 IMAGE_PATH=/data/test.jpg ./serve_client.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_PATH="${ROOT_DIR}/satellite_client"
#SERVER_ADDRESS="${1:-${SERVER_ADDRESS:-127.0.0.1:8308}}"
SERVER_ADDRESS="${1:-${SERVER_ADDRESS:-192.168.3.70:8309}}"
DEVICE_ID="${2:-${DEVICE_ID:-10001}}"
IMAGE_PATH="${3:-${IMAGE_PATH:-}}"

# 移除已经解析的前三个位置参数，剩余参数原样传给客户端。
if (( $# >= 3 )); then
    shift 3
else
    shift "$#"
fi

if [[ ! "${DEVICE_ID}" =~ ^[1-9][0-9]*$ ]]; then
    echo "设备 ID 必须是大于 0 的整数: ${DEVICE_ID}" >&2
    exit 1
fi

if [[ -n "${IMAGE_PATH}" && ! -f "${IMAGE_PATH}" ]]; then
    echo "测试图片不存在: ${IMAGE_PATH}" >&2
    exit 1
fi

cd "${ROOT_DIR}"

echo "正在编译模拟卫星客户端..."
go build -o "${BINARY_PATH}" ./cmd/device

CLIENT_ARGS=(
    -server "${SERVER_ADDRESS}"
    -device "${DEVICE_ID}"
)

if [[ -n "${IMAGE_PATH}" ]]; then
    CLIENT_ARGS+=( -image "${IMAGE_PATH}" )
fi

echo "正在启动模拟卫星客户端: server=${SERVER_ADDRESS}, device=${DEVICE_ID}, image=${IMAGE_PATH:-自动生成}"
exec "${BINARY_PATH}" "${CLIENT_ARGS[@]}" "$@"
