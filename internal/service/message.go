package service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"
)

type MessageService struct {
	imageDirectory string
	maxImageSize   int64
}

type LocationReport struct {
	Longitude float64   `json:"longitude"`
	Latitude  float64   `json:"latitude"`
	Altitude  float64   `json:"altitude,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

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

func (s *MessageService) HandleLocationReport(deviceID uint64, body []byte) error {
	var report LocationReport

	if err := json.Unmarshal(body, &report); err != nil {
		return fmt.Errorf("decode location report: %w", err)
	}

	if report.Longitude < -180 || report.Longitude > 180 {
		return errors.New("longitude must be between -180 and 180")
	}

	if report.Latitude < -90 || report.Latitude > 90 {
		return errors.New("latitude must be between -90 and 90")
	}

	if report.Timestamp.IsZero() {
		report.Timestamp = time.Now()
	}

	log.Printf(
		"location report: device=%d longitude=%f latitude=%f altitude=%f time=%s",
		deviceID,
		report.Longitude,
		report.Latitude,
		report.Altitude,
		report.Timestamp.Format(time.RFC3339),
	)

	return nil
}
