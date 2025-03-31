package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BRO3886/konnect-exercise/internal/config"
	"github.com/BRO3886/konnect-exercise/internal/kafka"
	"github.com/BRO3886/konnect-exercise/internal/opensearch"
	"github.com/BRO3886/konnect-exercise/internal/queue"
	"github.com/BRO3886/konnect-exercise/internal/search"
	"github.com/BRO3886/konnect-exercise/internal/types"
)

var mode string

func init() {
	flag.StringVar(&mode, "mode", "ingest", "mode to run in")
	flag.Parse()
}

func main() {
	cfg := config.LoadConfig()

	ctx := context.Background()
	kafkaCfg := kafka.NewConfig(
		kafka.WithBrokers(cfg.Kafka.Brokers...),
		kafka.WithSyncProducer(), // comment to run async producer
		kafka.WithConsumeOldest(),
		kafka.WithTopics(cfg.Kafka.Topic.Name),
		kafka.WithRetry(
			cfg.Kafka.Retry.Max,
			time.Duration(cfg.Kafka.Retry.Backoff)*time.Millisecond,
		),
	)

	if mode == "index" {
		dequeuer, err := kafka.NewDequeuer(ctx, kafkaCfg)
		if err != nil {
			log.Fatalf("error starting kafka dequeuer: %v", err)
		}

		searcher, err := opensearch.New(ctx, cfg)
		if err != nil {
			log.Fatalf("error starting opensearch searcher: %v", err)
		}

		runIndexing(ctx, cfg, dequeuer, searcher)
	} else if mode == "ingest" {
		enqueuer, err := kafka.NewEnqueuer(ctx, kafkaCfg)
		if err != nil {
			log.Fatalf("error starting kafka enqueuer: %v", err)
		}
		// // --- uncomment when using async producer
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case err := <-enqueuer.Errors():
					if err != nil {
						log.Printf("[ingestion] error: %v", err)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
		// // ---
		runIngestion(ctx, cfg, enqueuer)
		// // --- uncomment when using async producer
		// wg.Wait()
		// // ---
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
		// comment when using async producer
		time.Sleep(time.Millisecond * 100)
		// ---
	}
	log.Printf("ingestion completed")
}
