package main

/*import (
	"context"
	"encoding/json"
	"log"
	"os/signal"
	"syscall"

	"satellite/internal/gateway"
	"satellite/internal/wire"
)

// main 启动示例卫星 UDP 网关并注册普通消息与 RPC 处理逻辑。
// 它监听 9000 端口，将完成分片重组的文字和图片通过可靠 UDP 原样回传，并注册设备报告 RPC；随后在系统信号上下文中运行收包、ACK、重传和重组维护循环。监听或服务发生致命错误时记录日志并退出。
func main() {
	server, err := gateway.Listen(":9000")
	if err != nil {
		log.Fatal(err)
	}

	// 该普通消息回调接收网关完成分片重组后的完整负载；测试环境中按消息类型记录大小，并可靠地原样回传文字或图片，用于验证设备侧下行分片、ACK 和重组流程。
	server.HandleMessages(func(deviceID uint64, messageType wire.MessageType, payload []byte) {
		switch messageType {
		case wire.TypeText:
			log.Printf("device=%d text_bytes=%d", deviceID, len(payload))
			if err := server.SendText(deviceID, string(payload)); err != nil {
				log.Printf("echo text to device=%d: %v", deviceID, err)
			}
		case wire.TypeImage:
			log.Printf("device=%d image_bytes=%d", deviceID, len(payload))
			if err := server.SendImage(deviceID, payload); err != nil {
				log.Printf("echo image to device=%d: %v", deviceID, err)
			}
		}
	})

	// 该 RPC 回调解析设备报告 JSON；成功结果由网关编码、分片并通过 ACK/重传机制回送。
	server.HandleRPC("POST", "/device/report", func(_ context.Context, body json.RawMessage) (any, error) {
		var report map[string]any
		if err := json.Unmarshal(body, &report); err != nil {
			return nil, err
		}
		log.Printf("device report: %v", report)
		return map[string]any{"accepted": true}, nil
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("satellite UDP gateway listening on %s", server.Addr())
	if err := server.Serve(ctx); err != nil {
		log.Fatal(err)
	}
}*/

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"satellite/internal/config"
	"satellite/internal/gateway"
	satrpc "satellite/internal/rpc"
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

	rpcRouter := createRPCRouter(messageService)

	/*
	   这里先声明 udpGateway，再创建 MessageHandler。

	   MessageHandler 中需要通过 udpGateway.SendResponse()
	   向设备发送 RPC 响应，因此使用闭包引用。
	*/
	var udpGateway *gateway.Gateway

	messageHandler := func(
		ctx context.Context,
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

		case wire.TypeRequest:
			return handleRPCRequest(
				ctx,
				udpGateway,
				rpcRouter,
				packet,
				payload,
			)

		case wire.TypeResponse:
			/*
			   如果服务端未来需要主动向客户端发起 RPC，
			   可以在这里根据 MessageID 匹配服务端等待中的请求。

			   当前 MVP 只实现：
			   客户端 -> 服务端 RPC Request
			   服务端 -> 客户端 RPC Response
			*/
			log.Printf(
				"received unexpected RPC response: device=%d message=%d size=%d",
				packet.DeviceID,
				packet.MessageID,
				len(payload),
			)

			return nil

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

	udpGateway, err = gateway.NewGateway(
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
	   同时触发 HTTP 服务和整个应用关闭。
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

	httpRouter := createHTTPRouter(
		cfg,
		udpGateway,
	)

	httpServer := &http.Server{
		Addr:              cfg.Server.HTTPAddress,
		Handler:           httpRouter,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf(
			"HTTP API listening on %s",
			cfg.Server.HTTPAddress,
		)

		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("HTTP server stopped unexpectedly: %v", err)
			stop()
		}
	}()

	log.Printf(
		"satellite server started: UDP=%s HTTP=%s",
		cfg.Server.UDPAddress,
		cfg.Server.HTTPAddress,
	)

	<-rootContext.Done()

	log.Println("shutdown signal received")

	/*
	   先关闭 UDP socket，解除 Serve() 中 ReadFromUDP 的阻塞。
	*/
	if err := udpGateway.Close(); err != nil {
		log.Printf("close UDP gateway failed: %v", err)
	}

	shutdownContext, shutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownContext); err != nil {
		log.Printf("graceful HTTP shutdown failed: %v", err)

		if closeErr := httpServer.Close(); closeErr != nil {
			log.Printf("force close HTTP server failed: %v", closeErr)
		}
	}

	log.Println("satellite server stopped")
}

/*
createRPCRouter 注册卫星设备可通过 UDP 调用的“类 HTTP 接口”。

当前示例包含：

	GET  /v1/ping
	POST /v1/location/report
	POST /v1/messages/text
*/
func createRPCRouter(
	messageService *service.MessageService,
) *satrpc.Router {
	router := satrpc.NewRouter()

	router.Handle(
		"GET",
		"/v1/ping",
		func(
			ctx context.Context,
			deviceID uint64,
			request *satrpc.Message,
		) (int, string, []byte, error) {
			response := map[string]any{
				"success":  true,
				"message":  "pong",
				"deviceId": deviceID,
				"serverTime": time.Now().UTC().Format(
					time.RFC3339,
				),
			}

			body, err := json.Marshal(response)
			if err != nil {
				return http.StatusInternalServerError,
					"application/json",
					nil,
					err
			}

			return http.StatusOK,
				"application/json",
				body,
				nil
		},
	)

	router.Handle(
		"POST",
		"/v1/location/report",
		func(
			ctx context.Context,
			deviceID uint64,
			request *satrpc.Message,
		) (int, string, []byte, error) {
			if len(request.Body) == 0 {
				return jsonRPCResponse(
					http.StatusBadRequest,
					map[string]any{
						"success": false,
						"message": "request body cannot be empty",
					},
				)
			}

			if err := messageService.HandleLocationReport(
				deviceID,
				request.Body,
			); err != nil {
				return jsonRPCResponse(
					http.StatusBadRequest,
					map[string]any{
						"success": false,
						"message": err.Error(),
					},
				)
			}

			return jsonRPCResponse(
				http.StatusOK,
				map[string]any{
					"success":  true,
					"accepted": true,
					"deviceId": deviceID,
				},
			)
		},
	)

	router.Handle(
		"POST",
		"/v1/messages/text",
		func(
			ctx context.Context,
			deviceID uint64,
			request *satrpc.Message,
		) (int, string, []byte, error) {
			var input struct {
				MessageID uint64 `json:"messageId"`
				Text      string `json:"text"`
			}

			if err := json.Unmarshal(request.Body, &input); err != nil {
				return jsonRPCResponse(
					http.StatusBadRequest,
					map[string]any{
						"success": false,
						"message": "invalid JSON request body",
					},
				)
			}

			input.Text = strings.TrimSpace(input.Text)

			if input.Text == "" {
				return jsonRPCResponse(
					http.StatusBadRequest,
					map[string]any{
						"success": false,
						"message": "text cannot be empty",
					},
				)
			}

			if err := messageService.ReceiveText(
				deviceID,
				input.MessageID,
				input.Text,
			); err != nil {
				return jsonRPCResponse(
					http.StatusBadRequest,
					map[string]any{
						"success": false,
						"message": err.Error(),
					},
				)
			}

			return jsonRPCResponse(
				http.StatusOK,
				map[string]any{
					"success":  true,
					"accepted": true,
				},
			)
		},
	)

	return router
}

func handleRPCRequest(
	ctx context.Context,
	udpGateway *gateway.Gateway,
	router *satrpc.Router,
	packet *wire.Packet,
	payload []byte,
) error {
	if udpGateway == nil {
		return errors.New("UDP gateway is not initialized")
	}

	request, err := satrpc.Decode(payload)
	if err != nil {
		log.Printf(
			"decode RPC request failed: device=%d message=%d error=%v",
			packet.DeviceID,
			packet.MessageID,
			err,
		)

		return sendRPCErrorResponse(
			udpGateway,
			packet,
			http.StatusBadRequest,
			"invalid RPC request",
			"",
		)
	}

	log.Printf(
		"received RPC request: device=%d message=%d method=%s path=%s requestId=%s",
		packet.DeviceID,
		packet.MessageID,
		request.Metadata.Method,
		request.Metadata.Path,
		request.Metadata.RequestID,
	)

	status, contentType, responseBody, dispatchErr :=
		router.Dispatch(
			ctx,
			packet.DeviceID,
			request,
		)

	if dispatchErr != nil {
		log.Printf(
			"dispatch RPC request failed: device=%d message=%d error=%v",
			packet.DeviceID,
			packet.MessageID,
			dispatchErr,
		)

		/*
		   Router.Dispatch 已经提供了面向客户端的安全响应体，
		   这里不把内部错误细节直接返回设备。
		*/
		if status == 0 {
			status = http.StatusInternalServerError
		}
	}

	responsePayload, err := satrpc.Encode(
		satrpc.Metadata{
			Status:      status,
			ContentType: contentType,
			RequestID:   request.Metadata.RequestID,
		},
		responseBody,
	)
	if err != nil {
		return err
	}

	/*
	   Response 使用与 Request 相同的 MessageID，
	   客户端可通过 MessageID 完成请求与响应的关联。
	*/
	if err := udpGateway.SendResponse(
		packet.DeviceID,
		packet.MessageID,
		responsePayload,
	); err != nil {
		return err
	}

	log.Printf(
		"RPC response queued: device=%d message=%d status=%d",
		packet.DeviceID,
		packet.MessageID,
		status,
	)

	return nil
}

// sendRPCErrorResponse 构造统一 JSON 错误正文和 RPC 元数据，并进入消息级 ACK/重传的可靠响应流程。
func sendRPCErrorResponse(
	udpGateway *gateway.Gateway,
	packet *wire.Packet,
	status int,
	message string,
	requestID string,
) error {
	body, err := json.Marshal(map[string]any{
		"success": false,
		"message": message,
	})
	if err != nil {
		return err
	}

	payload, err := satrpc.Encode(
		satrpc.Metadata{
			Status:      status,
			ContentType: "application/json",
			RequestID:   requestID,
		},
		body,
	)
	if err != nil {
		return err
	}

	return udpGateway.SendResponse(
		packet.DeviceID,
		packet.MessageID,
		payload,
	)
}

func jsonRPCResponse(
	status int,
	value any,
) (int, string, []byte, error) {
	body, err := json.Marshal(value)
	if err != nil {
		return http.StatusInternalServerError,
			"application/json",
			nil,
			err
	}

	return status, "application/json", body, nil
}

/*
createHTTPRouter 创建供普通互联网平台或运营后台调用的 HTTP API。

接口：

	GET  /health
	GET  /api/devices
	GET  /api/devices/:deviceID
	GET  /api/devices/:deviceID/messages/:messageID
	POST /api/devices/:deviceID/messages/text
	POST /api/devices/:deviceID/messages/image
*/
func createHTTPRouter(
	cfg config.Config,
	udpGateway *gateway.Gateway,
) *gin.Engine {
	router := gin.New()

	router.Use(
		gin.Logger(),
		gin.Recovery(),
	)

	router.GET(
		"/health",
		func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{
				"status":    "UP",
				"timestamp": time.Now().UTC().Format(time.RFC3339),
			})
		},
	)

	api := router.Group("/api")

	api.GET(
		"/devices",
		func(c *gin.Context) {
			sessions := udpGateway.ListSessions()

			result := make([]gin.H, 0, len(sessions))

			for _, session := range sessions {
				clientAddress := ""
				transportAddress := ""

				if session.ClientAddress != nil {
					clientAddress = session.ClientAddress.String()
				}
				if session.TransportAddress != nil {
					transportAddress = session.TransportAddress.String()
				}

				result = append(result, gin.H{
					"deviceId":         session.DeviceID,
					"sessionId":        session.SessionID,
					"clientAddress":    clientAddress,
					"transportAddress": transportAddress,
					"lastSeen":         session.LastSeen,
					"online":           true,
				})
			}

			c.JSON(http.StatusOK, gin.H{
				"items": result,
				"total": len(result),
			})
		},
	)

	api.GET(
		"/devices/:deviceID",
		func(c *gin.Context) {
			deviceID, ok := parseDeviceID(c)
			if !ok {
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"deviceId": deviceID,
				"online":   udpGateway.IsDeviceOnline(deviceID),
			})
		},
	)

	api.GET(
		"/devices/:deviceID/messages/:messageID",
		func(c *gin.Context) {
			deviceID, ok := parseDeviceID(c)
			if !ok {
				return
			}

			messageID, err := strconv.ParseUint(
				c.Param("messageID"),
				10,
				64,
			)
			if err != nil || messageID == 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "invalid message ID",
				})
				return
			}

			status, exists := udpGateway.GetDeliveryStatus(
				deviceID,
				messageID,
			)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{
					"success": false,
					"message": "delivery status not found",
				})
				return
			}

			c.JSON(http.StatusOK, status)
		},
	)

	api.POST(
		"/devices/:deviceID/messages/text",
		func(c *gin.Context) {
			deviceID, ok := parseDeviceID(c)
			if !ok {
				return
			}

			var request struct {
				Text string `json:"text" binding:"required"`
			}

			if err := c.ShouldBindJSON(&request); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "invalid JSON request: " + err.Error(),
				})
				return
			}

			request.Text = strings.TrimSpace(request.Text)

			if request.Text == "" {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "text cannot be empty",
				})
				return
			}

			if len([]byte(request.Text)) >
				cfg.Gateway.MaxMessageSize {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{
					"success": false,
					"message": "text exceeds configured message size limit",
				})
				return
			}

			messageID, err := udpGateway.SendReliable(
				deviceID,
				wire.TypeText,
				[]byte(request.Text),
			)
			if err != nil {
				c.JSON(http.StatusConflict, gin.H{
					"success": false,
					"message": err.Error(),
				})
				return
			}

			/*
			   202 表示服务端已经接收并开始发送，
			   不代表卫星设备已经成功收到。

			   设备是否收到，需要通过投递状态接口查询。
			*/
			c.JSON(http.StatusAccepted, gin.H{
				"success":   true,
				"deviceId":  deviceID,
				"messageId": messageID,
				"state":     gateway.DeliveryPending,
			})
		},
	)

	api.POST(
		"/devices/:deviceID/messages/image",
		func(c *gin.Context) {
			deviceID, ok := parseDeviceID(c)
			if !ok {
				return
			}

			/*
			   给 multipart/form-data 的边界和请求头预留 1 MiB，
			   真正的图片大小仍通过 LimitReader 单独限制。
			*/
			maxHTTPBodySize := cfg.Storage.MaxImageSize +
				1024*1024

			c.Request.Body = http.MaxBytesReader(
				c.Writer,
				c.Request.Body,
				maxHTTPBodySize,
			)

			file, fileHeader, err := c.Request.FormFile("file")
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "multipart field 'file' is required",
				})
				return
			}
			defer file.Close()

			imageData, err := io.ReadAll(
				io.LimitReader(
					file,
					cfg.Storage.MaxImageSize+1,
				),
			)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "cannot read uploaded file",
				})
				return
			}

			if len(imageData) == 0 {
				c.JSON(http.StatusBadRequest, gin.H{
					"success": false,
					"message": "uploaded file cannot be empty",
				})
				return
			}

			if int64(len(imageData)) >
				cfg.Storage.MaxImageSize {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{
					"success": false,
					"message": "image exceeds configured size limit",
				})
				return
			}

			if len(imageData) > cfg.Gateway.MaxMessageSize {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{
					"success": false,
					"message": "image exceeds UDP message size limit",
				})
				return
			}

			/*
			   这里只做基础 MIME 检测。

			   生产环境建议：
			   1. 检查真实图片文件签名；
			   2. 对图片进行解码验证；
			   3. 限制图片像素尺寸；
			   4. 清除 EXIF 敏感信息；
			   5. 压缩后再通过卫星链路发送。
			*/
			detectedContentType := http.DetectContentType(imageData)

			if !strings.HasPrefix(
				detectedContentType,
				"image/",
			) {
				c.JSON(http.StatusUnsupportedMediaType, gin.H{
					"success": false,
					"message": "uploaded file is not a supported image",
				})
				return
			}

			messageID, err := udpGateway.SendReliable(
				deviceID,
				wire.TypeImage,
				imageData,
			)
			if err != nil {
				c.JSON(http.StatusConflict, gin.H{
					"success": false,
					"message": err.Error(),
				})
				return
			}

			// 与文字下发相同，HTTP 202 只表示网关已受理发送；设备确认结果需通过投递状态接口查询。
			c.JSON(http.StatusAccepted, gin.H{
				"success":     true,
				"deviceId":    deviceID,
				"messageId":   messageID,
				"state":       gateway.DeliveryPending,
				"filename":    fileHeader.Filename,
				"contentType": detectedContentType,
				"size":        len(imageData),
			})
		},
	)

	return router
}

// parseDeviceID 解析必须为正整数的设备路径参数，失败时直接写入 HTTP 400 响应。
func parseDeviceID(c *gin.Context) (uint64, bool) {
	deviceID, err := strconv.ParseUint(
		c.Param("deviceID"),
		10,
		64,
	)
	if err != nil || deviceID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"success": false,
			"message": "invalid device ID",
		})
		return 0, false
	}

	return deviceID, true
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
