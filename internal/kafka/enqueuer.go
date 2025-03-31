package kafka

import (
	"context"
	"log"

	"github.com/BRO3886/konnect-exercise/internal/queue"
	"github.com/IBM/sarama"
)

type KafkaEnqueuer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	cfg           *Config
}

func NewEnqueuer(ctx context.Context, c *Config) (queue.Enqueuer, error) {
	syncProducer, err := sarama.NewSyncProducer(c.GetBrokers(), c.GetConfig())
	if err != nil {
		return nil, err
	}

	asyncProducer, err := sarama.NewAsyncProducer(c.GetBrokers(), c.GetConfig())
	if err != nil {
		return nil, err
	}

	return &KafkaEnqueuer{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		cfg:           c,
	}, nil
}

func (k *KafkaEnqueuer) Enqueue(ctx context.Context, topic string, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	if k.cfg.IsSync() {
		return k.enqueueSync(ctx, msg)
	}
	ch := make(chan error)
	k.enqueueAsync(ctx, msg, ch)
	return <-ch
}

func (k *KafkaEnqueuer) enqueueSync(ctx context.Context, msg *sarama.ProducerMessage) error {
	partition, offset, err := k.syncProducer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("[kafka] [%s] message sent to partition %d with offset %d", msg.Topic, partition, offset)
	return nil
}

func (k *KafkaEnqueuer) enqueueAsync(ctx context.Context, msg *sarama.ProducerMessage, ch chan error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func(ctx context.Context) {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				ch <- nil
				return
			case err := <-k.asyncProducer.Errors():
				ch <- err
				return
			}
		}
	}(ctx)
	k.asyncProducer.Input() <- msg
}

func (k *KafkaEnqueuer) Close() error {
	if err := k.syncProducer.Close(); err != nil {
		return err
	}
	if err := k.asyncProducer.Close(); err != nil {
		return err
	}
	return nil
}
