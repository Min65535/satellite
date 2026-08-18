package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Gateway GatewayConfig `yaml:"gateway"`
	Storage StorageConfig `yaml:"storage"`
}

type ServerConfig struct {
	UDPAddress  string `yaml:"udp_address"`
	HTTPAddress string `yaml:"http_address"`
}

type GatewayConfig struct {
	WorkerCount       int           `yaml:"worker_count"`
	ReadBufferSize    int           `yaml:"read_buffer_size"`
	WriteBufferSize   int           `yaml:"write_buffer_size"`
	SessionTimeout    time.Duration `yaml:"session_timeout"`
	ReassemblyTimeout time.Duration `yaml:"reassembly_timeout"`
	AckTimeout        time.Duration `yaml:"ack_timeout"`
	MaxRetries        int           `yaml:"max_retries"`
	MaxMessageSize    int           `yaml:"max_message_size"`
}

type StorageConfig struct {
	ImageDirectory string `yaml:"image_directory"`
	MaxImageSize   int64  `yaml:"max_image_size"`
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			UDPAddress:  ":9000",
			HTTPAddress: ":8080",
		},
		Gateway: GatewayConfig{
			WorkerCount:       128,
			ReadBufferSize:    4 * 1024 * 1024,
			WriteBufferSize:   4 * 1024 * 1024,
			SessionTimeout:    3 * time.Minute,
			ReassemblyTimeout: 2 * time.Minute,
			AckTimeout:        8 * time.Second,
			MaxRetries:        4,
			MaxMessageSize:    5 * 1024 * 1024,
		},
		Storage: StorageConfig{
			ImageDirectory: "./storage/images",
			MaxImageSize:   5 * 1024 * 1024,
		},
	}
}

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

func applyDefaults(cfg *Config) {
	defaults := Default()

	if cfg.Server.UDPAddress == "" {
		cfg.Server.UDPAddress = defaults.Server.UDPAddress
	}
	if cfg.Server.HTTPAddress == "" {
		cfg.Server.HTTPAddress = defaults.Server.HTTPAddress
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
	if cfg.Gateway.AckTimeout <= 0 {
		cfg.Gateway.AckTimeout = defaults.Gateway.AckTimeout
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

func validate(cfg Config) error {
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
