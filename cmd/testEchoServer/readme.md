# windows
```shell
./testEchoServer.exe -config .\config.yaml
```

## linux
```shell
# 遇到命令失败、未定义变量或管道错误时立即退出。
set -e

cd satellite
go build -o testEchoServer ./cmd/testEchoServer

nohup ./testEchoServer -config ./config.yaml > output.log 2>&1 &

exit 0;
```