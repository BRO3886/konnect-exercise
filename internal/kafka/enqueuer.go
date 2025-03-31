package kafka

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

type KafkaEnqueuer struct {
	syncProducer  sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
	cfg           *Config
	producerErrs  chan *sarama.ProducerError
}

func NewEnqueuer(ctx context.Context, c *Config) (*KafkaEnqueuer, error) {
	syncProducer, err := sarama.NewSyncProducer(c.GetBrokers(), c.GetConfig())
	if err != nil {
		return nil, err
	}

	asyncProducer, err := sarama.NewAsyncProducer(c.GetBrokers(), c.GetConfig())
	if err != nil {
		return nil, err
	}

	k := &KafkaEnqueuer{
		syncProducer:  syncProducer,
		asyncProducer: asyncProducer,
		cfg:           c,
	}

	if !c.IsSync() {
		k.producerErrs = make(chan *sarama.ProducerError)
		go func() {
			defer close(k.producerErrs)
			for err := range k.asyncProducer.Errors() {
				k.producerErrs <- err
			}
		}()
	}

	return k, nil
}

func (k *KafkaEnqueuer) Enqueue(ctx context.Context, topic string, data []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	if k.cfg.IsSync() {
		return k.enqueueSync(ctx, msg)
	}

	k.enqueueAsync(ctx, msg)
	return nil
}

func (k *KafkaEnqueuer) Errors() <-chan *sarama.ProducerError {
	if k.cfg.IsSync() {
		return nil
	}
	return k.producerErrs
}

func (k *KafkaEnqueuer) enqueueSync(ctx context.Context, msg *sarama.ProducerMessage) error {
	partition, offset, err := k.syncProducer.SendMessage(msg)
	if err != nil {
		return err
	}
	log.Printf("[kafka] [%s] message sent to partition %d with offset %d", msg.Topic, partition, offset)
	return nil
}

func (k *KafkaEnqueuer) enqueueAsync(ctx context.Context, msg *sarama.ProducerMessage) {
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
