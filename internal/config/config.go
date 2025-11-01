package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server ServerConfig `yaml:"server"`
}

type ServerConfig struct {
	Address                     string `yaml:"address"`
	WALPath                     string `yaml:"wal_path"`
	TimeoutSeconds              int    `yaml:"timeout_seconds"`
	TimeoutCheckIntervalSeconds int    `yaml:"timeout_check_interval_seconds"`
}

func (c *ServerConfig) GetTimeout() time.Duration {
	return time.Duration(c.TimeoutSeconds) * time.Second
}

func (c *ServerConfig) GetTimeoutCheckInterval() time.Duration {
	return time.Duration(c.TimeoutCheckIntervalSeconds) * time.Second
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return &cfg, nil
}
