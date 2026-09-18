// device1 是用于验证服务端原始 UDP 回显模式的最小客户端。
package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"time"
)

func main() {
	serverAddress := flag.String("server", "127.0.0.1:8308", "UDP 回显服务端地址")
	message := flag.String("message", "hello satellite UDP echo", "需要发送的任意文字内容")
	timeout := flag.Duration("timeout", 5*time.Second, "等待服务端回显的超时时间")
	flag.Parse()

	address, err := net.ResolveUDPAddr("udp", *serverAddress)
	if err != nil {
		log.Fatalf("解析服务端地址失败: %v", err)
	}

	conn, err := net.DialUDP("udp", nil, address)
	if err != nil {
		log.Fatalf("连接 UDP 服务端失败: %v", err)
	}
	defer conn.Close()

	request := []byte(*message)
	if _, err := conn.Write(request); err != nil {
		log.Fatalf("发送 UDP 数据失败: %v", err)
	}
	log.Printf("已发送: server=%s bytes=%d content=%q", address, len(request), request)

	if err := conn.SetReadDeadline(time.Now().Add(*timeout)); err != nil {
		log.Fatalf("设置读取超时失败: %v", err)
	}

	response := make([]byte, 65535)
	length, err := conn.Read(response)
	if err != nil {
		log.Fatalf("等待 UDP 回显失败: %v", err)
	}
	response = response[:length]

	log.Printf("已收到回显: bytes=%d content=%q", len(response), response)
	if string(response) != string(request) {
		log.Fatalf("回显内容不一致: sent=%q received=%q", request, response)
	}

	fmt.Println("UDP 回显测试成功")
}
