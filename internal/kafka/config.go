package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	cfg     *sarama.Config
	brokers []string
	topics  []string
	sync    bool
}

type ConfigOpts func(*Config)

func WithSyncProducer() ConfigOpts {
	return func(c *Config) {
		c.sync = true
		c.cfg.Producer.RequiredAcks = sarama.WaitForAll
	}
}

func WithRetry(maxRetries int, backoff time.Duration) ConfigOpts {
	return func(c *Config) {
		c.cfg.Producer.Retry.Max = maxRetries
		c.cfg.Producer.Retry.Backoff = backoff
	}
}

func WithBrokers(brokers ...string) ConfigOpts {
	return func(c *Config) {
		c.brokers = brokers
	}
}

func WithTopics(topics ...string) ConfigOpts {
	return func(c *Config) {
		c.topics = topics
	}
}

func WithConsumeOldest() ConfigOpts {
	return func(c *Config) {
		c.cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
}

func NewConfig(opts ...ConfigOpts) *Config {
	s := sarama.NewConfig()
	s.Version = sarama.V2_8_0_0
	s.Producer.RequiredAcks = sarama.WaitForLocal
	s.Producer.Return.Successes = true
	s.Producer.Return.Errors = true
	cfg := &Config{
		cfg: s,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func (c *Config) IsSync() bool {
	return c.sync
}

func (c *Config) GetTopics() []string {
	return c.topics
}

func (c *Config) AddTopics(topics ...string) {
	c.topics = append(c.topics, topics...)
}

func (c *Config) GetBrokers() []string {
	return c.brokers
}

func (c *Config) GetConfig() *sarama.Config {
	return c.cfg
}
