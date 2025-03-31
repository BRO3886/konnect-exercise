package kafka

import (
	"context"

	"github.com/BRO3886/konnect-exercise/internal/queue"
	"github.com/IBM/sarama"
)

type KafkaDequeuer struct {
	consumerGroups map[string]sarama.ConsumerGroup
	cfg            *Config
}

func NewDequeuer(ctx context.Context, c *Config) (queue.Dequeuer, error) {
	consumerGroups := make(map[string]sarama.ConsumerGroup)
	for _, topic := range c.GetTopics() {
		consumerGroup, err := sarama.NewConsumerGroup(c.GetBrokers(), topic, c.GetConfig())
		if err != nil {
			return nil, err
		}
		consumerGroups[topic] = consumerGroup
	}
	return &KafkaDequeuer{
		consumerGroups: consumerGroups,
		cfg:            c,
	}, nil
}

func (k *KafkaDequeuer) Dequeue(ctx context.Context, topic string, handler queue.MessageHandler) error {
	if _, ok := k.consumerGroups[topic]; !ok {
		consumerGroup, err := sarama.NewConsumerGroup(k.cfg.GetBrokers(), topic, k.cfg.GetConfig())
		if err != nil {
			return err
		}
		k.consumerGroups[topic] = consumerGroup
	}
	consumerGroup := k.consumerGroups[topic]

	return consumerGroup.Consume(ctx, []string{topic}, NewConsumerGroupHandler(handler))
}

func (k *KafkaDequeuer) Close() error {
	return nil
}
