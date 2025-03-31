package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/BRO3886/konnect-exercise/internal/config"
	"github.com/BRO3886/konnect-exercise/internal/kafka"
	"github.com/BRO3886/konnect-exercise/internal/opensearch"
	"github.com/BRO3886/konnect-exercise/internal/queue"
	"github.com/BRO3886/konnect-exercise/internal/search"
	"github.com/BRO3886/konnect-exercise/internal/types"
)

var mode string
var workers int

func init() {
	flag.StringVar(&mode, "mode", "ingest", "mode to run in")
	flag.IntVar(&workers, "workers", 1, "number of workers to run")
	flag.Parse()
}

func main() {
	cfg := config.LoadConfig()

	ctx := context.Background()
	kafkaCfg := kafka.NewConfig(
		kafka.WithBrokers(cfg.Kafka.Brokers...),
		kafka.WithSyncProducer(),
		kafka.WithConsumeOldest(),
		kafka.WithRetry(
			cfg.Kafka.Retry.Max,
			time.Duration(cfg.Kafka.Retry.Backoff)*time.Millisecond,
		),
	)

	if mode == "ingest" {
		enqueuer, err := kafka.NewEnqueuer(ctx, kafkaCfg)
		if err != nil {
			log.Fatalf("error starting kafka enqueuer: %v", err)
		}
		runIngestion(ctx, cfg, enqueuer)
	} else if mode == "index" {
		dequeuer, err := kafka.NewDequeuer(ctx, kafkaCfg)
		if err != nil {
			log.Fatalf("error starting kafka dequeuer: %v", err)
		}

		searcher, err := opensearch.New(ctx, cfg)
		if err != nil {
			log.Fatalf("error starting opensearch searcher: %v", err)
		}

		runIndexing(ctx, cfg, dequeuer, searcher)
	}
}

func runIndexing(
	ctx context.Context,
	cfg *config.Config,
	dequeuer queue.Dequeuer,
	search search.Searcher,
) {
	log.Printf("started indexing")
	defer dequeuer.Close()
	if err := dequeuer.Dequeue(ctx, cfg.Kafka.Topic.Name, func(ctx context.Context, data []byte) error {
		var event types.Event
		if err := json.Unmarshal(data, &event); err != nil {
			log.Printf("[handler] error unmarshalling event: %v", err)
			return err
		}

		document, err := types.GetIndexableDoc(event)
		if err != nil {
			log.Printf("[handler] error parsing object and index: %v", err)
			return err
		}

		if document == nil {
			log.Printf("[handler] document is nil")
			return nil
		}

		if document.DeIndex {
			log.Printf("[handler] deindexing document %s", document.Id)
			return search.DeIndex(ctx, document.Id)
		}

		return search.Index(ctx, *document)
	}); err != nil {
		log.Printf("[handler] error dequeuing events: %v", err)
	}
}

func runIngestion(ctx context.Context, cfg *config.Config, enqueuer queue.Enqueuer) {
	log.Printf("started ingestion")
	// enqueue to kafka
	defer enqueuer.Close()
	data, err := os.ReadFile("stream.jsonl")
	if err != nil {
		log.Fatalf("failed to read stream file: %v", err)
	}
	lines := strings.Split(string(data), "\n")

	var event types.Event
	for i, line := range lines {
		bytes := []byte(line)
		if err := json.Unmarshal(bytes, &event); err != nil {
			log.Printf("error unmarshalling event %d: %v", i, err)
			continue
		}
		// validations if any
		if time.UnixMilli(event.TimeStamp).After(time.Now()) {
			log.Printf("event %d is in the future", i)
			continue
		}
		if err := enqueuer.Enqueue(ctx, cfg.Kafka.Topic.Name, bytes); err != nil {
			log.Printf("error enqueuing event %d: %v", i, err)
		}
		log.Printf("enqueued event %d", i)
		time.Sleep(time.Millisecond * 100)
	}
	log.Printf("ingestion completed")
}
