package service

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

// MessageService 处理重组完成的设备业务消息；当前文字仅记录日志，图片写入本地文件系统。
type MessageService struct {
	imageDirectory string // imageDirectory 是按设备 ID 建立子目录的图片存储根目录。
	maxImageSize   int64  // maxImageSize 是写文件前再次执行的单图字节上限。
}

// NewMessageService 校验存储参数并预先创建图片根目录。
func NewMessageService(imageDirectory string, maxImageSize int64) (*MessageService, error) {
	if imageDirectory == "" {
		return nil, errors.New("image directory cannot be empty")
	}

	if maxImageSize <= 0 {
		return nil, errors.New("maximum image size must be positive")
	}

	if err := os.MkdirAll(imageDirectory, 0755); err != nil {
		return nil, fmt.Errorf("create image directory: %w", err)
	}

	return &MessageService{
		imageDirectory: imageDirectory,
		maxImageSize:   maxImageSize,
	}, nil
}

// ReceiveText 接收一条已完成 UDP 重组和网关去重的文字消息。
// 当前实现拒绝空文本并记录日志，不持久化内容；生产环境可用设备 ID 与消息 ID 作为幂等存储依据。
func (s *MessageService) ReceiveText(deviceID uint64, messageID uint64, text string) error {
	if text == "" {
		return errors.New("text message cannot be empty")
	}

	log.Printf(
		"received text: device=%d message=%d text=%q",
		deviceID,
		messageID,
		text,
	)

	/*
	   生产环境可在这里写入数据库，例如：

	   messages(
	       device_id,
	       message_id,
	       direction,
	       message_type,
	       content,
	       created_at
	   )
	*/

	return nil
}

// ReceiveBizRequest 接收一条已完成分片重组、CRC 校验和消息去重的自定义业务请求。
// payload 的具体编码由设备与服务端业务协议约定；当前实现保留原始字节并记录接收日志。
func (s *MessageService) ReceiveBizRequest(deviceID uint64, messageID uint64, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("business request payload cannot be empty")
	}

	log.Printf(
		"received business request: device=%d message=%d size=%d payload=%x",
		deviceID,
		messageID,
		len(payload),
		payload,
	)

	return nil
}

// SaveImage 将重组后的完整图片按“根目录/设备 ID/消息 ID_时间戳.bin”保存并返回路径。
// 它在落盘前独立限制图片大小；当前不识别格式，也不提供跨进程去重。
func (s *MessageService) SaveImage(deviceID uint64, messageID uint64, data []byte) (string, error) {
	if len(data) == 0 {
		return "", errors.New("image data cannot be empty")
	}

	if int64(len(data)) > s.maxImageSize {
		return "", errors.New("image exceeds configured size limit")
	}

	deviceDirectory := filepath.Join(
		s.imageDirectory,
		fmt.Sprintf("%d", deviceID),
	)

	if err := os.MkdirAll(deviceDirectory, 0755); err != nil {
		return "", fmt.Errorf("create device image directory: %w", err)
	}

	/*
	   当前协议没有携带原始图片扩展名，因此使用.bin保存。

	   生产版本建议在图片Payload中增加：
	   - MIME类型
	   - 原始文件名
	   - SHA-256
	   - 文件总大小

	   并根据真实文件签名判断扩展名，而不是直接信任客户端。
	*/
	filename := fmt.Sprintf(
		"%d_%d.bin",
		messageID,
		time.Now().Unix(),
	)

	filePath := filepath.Join(deviceDirectory, filename)

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return "", fmt.Errorf("save image: %w", err)
	}

	log.Printf(
		"received image: device=%d message=%d size=%d path=%s",
		deviceID,
		messageID,
		len(data),
		filePath,
	)

	return filePath, nil
}
