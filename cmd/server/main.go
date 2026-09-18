package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"satellite/internal/config"
	"satellite/internal/gateway"
	"satellite/internal/service"
	"satellite/internal/wire"
)

func main() {
	configPath := flag.String(
		"config",
		"./config.yaml",
		"path to configuration file",
	)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load configuration failed: %v", err)
	}

	rootContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	messageService, err := service.NewMessageService(
		cfg.Storage.ImageDirectory,
		cfg.Storage.MaxImageSize,
	)
	if err != nil {
		log.Fatalf("create message service failed: %v", err)
	}

	messageHandler := func(
		_ context.Context,
		packet *wire.Packet,
		payload []byte,
	) error {
		switch packet.Type {
		case wire.TypeText:
			// 网关已在完整重组后返回空载荷 ACK；业务层只处理正文，不再把原消息回传给设备。
			return messageService.ReceiveText(
				packet.DeviceID,
				packet.MessageID,
				string(payload),
			)

		case wire.TypeImage:
			filePath, err := messageService.SaveImage(
				packet.DeviceID,
				packet.MessageID,
				payload,
			)
			if err != nil {
				return err
			}

			log.Printf(
				"image stored successfully: device=%d message=%d path=%s",
				packet.DeviceID,
				packet.MessageID,
				filePath,
			)

			// 网关返回的空载荷 ACK 已表示图片完整到达；这里不再回传图片原始内容。
			return nil

		case wire.TypeBizRequest:
			// payload 是已完成全部分片重组的原始业务数据；各分片 ACK 已由网关自动返回。
			return messageService.ReceiveBizRequest(
				packet.DeviceID,
				packet.MessageID,
				payload,
			)

		case wire.TypeError:
			log.Printf(
				"received device error: device=%d message=%d content=%s",
				packet.DeviceID,
				packet.MessageID,
				string(payload),
			)

			return nil

		default:
			return errors.New("unsupported application message type")
		}
	}

	udpGateway, err := gateway.NewGateway(
		rootContext,
		cfg.Server.UDPAddress,
		gateway.Options{
			WorkerCount:       cfg.Gateway.WorkerCount,
			ReadBufferSize:    cfg.Gateway.ReadBufferSize,
			WriteBufferSize:   cfg.Gateway.WriteBufferSize,
			SessionTimeout:    cfg.Gateway.SessionTimeout,
			ReassemblyTimeout: cfg.Gateway.ReassemblyTimeout,
			AckTimeout:        cfg.Gateway.AckTimeout,
			MaxRetries:        cfg.Gateway.MaxRetries,
			MaxMessageSize:    cfg.Gateway.MaxMessageSize,
			ProxyProtocolV2:   cfg.Gateway.ProxyProtocolV2,
		},
		messageHandler,
	)
	if err != nil {
		log.Fatalf("create UDP gateway failed: %v", err)
	}

	defer func() {
		if err := udpGateway.Close(); err != nil &&
			!errors.Is(err, netClosedError()) {
			log.Printf("close UDP gateway failed: %v", err)
		}
	}()

	/*
	   启动 UDP 服务。

	   如果 UDP 服务异常退出，则调用 stop()，
	   触发整个应用关闭。
	*/
	go func() {
		log.Printf(
			"UDP gateway listening on %s",
			cfg.Server.UDPAddress,
		)

		if err := udpGateway.Serve(); err != nil {
			log.Printf("UDP gateway stopped unexpectedly: %v", err)
			stop()
		}
	}()

	log.Printf(
		"satellite server started: UDP=%s",
		cfg.Server.UDPAddress,
	)

	<-rootContext.Done()

	log.Println("shutdown signal received")

	/*
	   先关闭 UDP socket，解除 Serve() 中 ReadFromUDP 的阻塞。
	*/
	if err := udpGateway.Close(); err != nil {
		log.Printf("close UDP gateway failed: %v", err)
	}

	log.Println("satellite server stopped")
}

/*
netClosedError 仅用于兼容 Gateway.Close() 的错误判断。

当前 Gateway.Close() 内部通常返回 net.ErrClosed 或 nil。
为了避免 main.go 增加不必要的 net 包依赖，这里返回一个不会匹配
其他错误的占位错误。实际上重复关闭已由 Gateway.closeOnce 处理，
正常情况下 Gateway.Close() 不会产生需要关注的错误。
*/
func netClosedError() error {
	return errors.New("use of closed network connection")
}
