package kafka

import (
	"log"

	"github.com/BRO3886/konnect-exercise/internal/queue"
	"github.com/IBM/sarama"
)

type ConsumerGroupHandler struct {
	handler queue.MessageHandler
}

func NewConsumerGroupHandler(handler queue.MessageHandler) sarama.ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		handler: handler,
	}
}

// Cleanup implements sarama.ConsumerGroupHandler.
func (c *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim implements sarama.ConsumerGroupHandler.
func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("[kafka] [%s] consuming claims", claim.Topic())
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic: %v", r)
		}
	}()

	for message := range claim.Messages() {
		if err := c.handler(session.Context(), message.Value); err != nil {
			log.Printf("error handling message: %v", err)
			return err
		}
		session.MarkMessage(message, "")
	}
	return nil
}

// Setup implements sarama.ConsumerGroupHandler.
func (c *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}
