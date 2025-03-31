package config

import (
	"log"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type Config struct {
	Kafka struct {
		Brokers []string `koanf:"brokers"`
		Topic   struct {
			Name       string `koanf:"name"`
			Partitions int    `koanf:"partitions"`
		} `koanf:"topic"`
		ConsumerGroup string `koanf:"consumer_group"`
		Retry         struct {
			Max     int `koanf:"max"`
			Backoff int `koanf:"backoff"`
		} `koanf:"retry"`
	} `koanf:"kafka"`
	Opensearch struct {
		URLs       []string `koanf:"urls"`
		Username   string   `koanf:"username"`
		Password   string   `koanf:"password"`
		MaxRetries int      `koanf:"max_retries"`
		Index      struct {
			Name          string `koanf:"name"`
			BuffSize      int    `koanf:"buff_size"`
			FlushInterval int    `koanf:"flush_interval"`
		} `koanf:"index"`
	} `koanf:"opensearch"`
}

var k = koanf.New(".")
var config *Config

func LoadConfig() *Config {
	f := file.Provider("configs/config.yaml")
	if err := k.Load(f, yaml.Parser()); err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	if err := k.Unmarshal("", &config); err != nil {
		log.Fatalf("error unmarshalling config: %v", err)
	}

	return config
}
