package queue

import "context"

type Enqueuer interface {
	Enqueue(ctx context.Context, topic string, data []byte) error
	Close() error
}

type MessageHandler func(ctx context.Context, data []byte) error

type Dequeuer interface {
	Dequeue(ctx context.Context, topic string, handler MessageHandler) error
	Close() error
}
