package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 汇总进程监听地址、UDP 网关资源限制和上行图片落盘策略。
type Config struct {
	Server  ServerConfig  `yaml:"server"`  // Server 配置对外提供的 UDP 监听端点。
	Gateway GatewayConfig `yaml:"gateway"` // Gateway 配置 UDP 并发、超时、重传与消息大小上限。
	Storage StorageConfig `yaml:"storage"` // Storage 配置设备上行图片的本地文件存储。
}

// ServerConfig 定义进程的 UDP 设备入口监听地址。
type ServerConfig struct {
	UDPAddress string `yaml:"udp_address"` // UDPAddress 是设备数据报网关的监听地址。
}

// GatewayConfig 控制 UDP 网关的容量与可靠传输时序。
type GatewayConfig struct {
	WorkerCount       int           `yaml:"worker_count"`       // WorkerCount 是并发处理数据报的上限，满载时新包会被丢弃并依赖发送端重传。
	ReadBufferSize    int           `yaml:"read_buffer_size"`   // ReadBufferSize 是操作系统 UDP 接收缓冲区的期望字节数。
	WriteBufferSize   int           `yaml:"write_buffer_size"`  // WriteBufferSize 是操作系统 UDP 发送缓冲区的期望字节数。
	SessionTimeout    time.Duration `yaml:"session_timeout"`    // SessionTimeout 是设备最后活动后仍被视为在线的时长。
	ReassemblyTimeout time.Duration `yaml:"reassembly_timeout"` // ReassemblyTimeout 是未完成分片组在最后一次新分片后保留的时长。
	CleanupInterval   time.Duration `yaml:"cleanup_interval"`   // CleanupInterval 是扫描并清理过期会话、重组和历史状态的周期。
	AckTimeout        time.Duration `yaml:"ack_timeout"`        // AckTimeout 是服务端发送分片后首次等待 ACK 的时长。
	MaxAckTimeout     time.Duration `yaml:"max_ack_timeout"`    // MaxAckTimeout 是指数退避后的最大 ACK 等待时长。
	SendWindowSize    int           `yaml:"send_window_size"`   // SendWindowSize 是每条消息允许同时在途的分片数。
	MaxRetries        int           `yaml:"max_retries"`        // MaxRetries 是单个分片 ACK 超时后的最大重发次数。
	MaxMessageSize    int           `yaml:"max_message_size"`   // MaxMessageSize 是重组或下发的一条逻辑消息允许的最大总字节数。
	ProxyProtocolV2   bool          `yaml:"proxy_protocol_v2"`  // ProxyProtocolV2 要求每个 FRP UDP 数据报携带 Proxy Protocol v2 头并从中提取真实公网地址。
}

// StorageConfig 定义上行图片在本机文件系统中的存储策略。
type StorageConfig struct {
	ImageDirectory string `yaml:"image_directory"` // ImageDirectory 是图片根目录，实际文件再按设备 ID 分目录保存。
	MaxImageSize   int64  `yaml:"max_image_size"`  // MaxImageSize 是单张上行图片允许落盘的最大字节数。
}

// Default 返回可直接运行的完整默认配置；Load 会以它为基底补齐缺省或非正数配置项。
func Default() Config {
	return Config{
		Server: ServerConfig{
			UDPAddress: ":9000",
		},
		Gateway: GatewayConfig{
			WorkerCount:       128,
			ReadBufferSize:    4 * 1024 * 1024,
			WriteBufferSize:   4 * 1024 * 1024,
			SessionTimeout:    3 * time.Minute,
			ReassemblyTimeout: 2 * time.Minute,
			CleanupInterval:   30 * time.Second,
			AckTimeout:        8 * time.Second,
			MaxAckTimeout:     30 * time.Second,
			SendWindowSize:    4,
			MaxRetries:        4,
			MaxMessageSize:    5 * 1024 * 1024,
		},
		Storage: StorageConfig{
			ImageDirectory: "./storage/images",
			MaxImageSize:   5 * 1024 * 1024,
		},
	}
}

// Load 在默认配置之上解析 YAML、补齐空值并校验跨字段大小约束。
// 文件读取、YAML 解码或校验失败时返回当前配置和带上下文的错误。
func Load(path string) (Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("decode config file: %w", err)
	}

	applyDefaults(&cfg)

	if err := validate(cfg); err != nil {
		return cfg, err
	}

	return cfg, nil
}

// applyDefaults 将字符串空值和数值、时长的非正值视为未配置，并替换为默认值。
func applyDefaults(cfg *Config) {
	defaults := Default()

	if cfg.Server.UDPAddress == "" {
		cfg.Server.UDPAddress = defaults.Server.UDPAddress
	}
	if cfg.Gateway.WorkerCount <= 0 {
		cfg.Gateway.WorkerCount = defaults.Gateway.WorkerCount
	}
	if cfg.Gateway.ReadBufferSize <= 0 {
		cfg.Gateway.ReadBufferSize = defaults.Gateway.ReadBufferSize
	}
	if cfg.Gateway.WriteBufferSize <= 0 {
		cfg.Gateway.WriteBufferSize = defaults.Gateway.WriteBufferSize
	}
	if cfg.Gateway.SessionTimeout <= 0 {
		cfg.Gateway.SessionTimeout = defaults.Gateway.SessionTimeout
	}
	if cfg.Gateway.ReassemblyTimeout <= 0 {
		cfg.Gateway.ReassemblyTimeout = defaults.Gateway.ReassemblyTimeout
	}
	if cfg.Gateway.CleanupInterval <= 0 {
		cfg.Gateway.CleanupInterval = defaults.Gateway.CleanupInterval
	}
	if cfg.Gateway.AckTimeout <= 0 {
		cfg.Gateway.AckTimeout = defaults.Gateway.AckTimeout
	}
	if cfg.Gateway.MaxAckTimeout <= 0 {
		cfg.Gateway.MaxAckTimeout = defaults.Gateway.MaxAckTimeout
	}
	if cfg.Gateway.SendWindowSize <= 0 {
		cfg.Gateway.SendWindowSize = defaults.Gateway.SendWindowSize
	}
	if cfg.Gateway.MaxRetries <= 0 {
		cfg.Gateway.MaxRetries = defaults.Gateway.MaxRetries
	}
	if cfg.Gateway.MaxMessageSize <= 0 {
		cfg.Gateway.MaxMessageSize = defaults.Gateway.MaxMessageSize
	}
	if cfg.Storage.ImageDirectory == "" {
		cfg.Storage.ImageDirectory = defaults.Storage.ImageDirectory
	}
	if cfg.Storage.MaxImageSize <= 0 {
		cfg.Storage.MaxImageSize = defaults.Storage.MaxImageSize
	}
}

// validate 限制内存型分片重组的单消息规模，并保证图片上限不超过 UDP 逻辑消息上限。
func validate(cfg Config) error {
	if cfg.Gateway.MaxAckTimeout < cfg.Gateway.AckTimeout {
		return fmt.Errorf("gateway.max_ack_timeout cannot be less than gateway.ack_timeout")
	}
	if cfg.Gateway.MaxMessageSize > 64*1024*1024 {
		return fmt.Errorf("gateway.max_message_size cannot exceed 64 MiB")
	}

	if cfg.Storage.MaxImageSize > int64(cfg.Gateway.MaxMessageSize) {
		return fmt.Errorf(
			"storage.max_image_size cannot exceed gateway.max_message_size",
		)
	}

	return nil
}
