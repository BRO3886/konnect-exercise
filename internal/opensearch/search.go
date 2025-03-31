package opensearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/BRO3886/konnect-exercise/internal/config"
	"github.com/BRO3886/konnect-exercise/internal/search"
	"github.com/BRO3886/konnect-exercise/internal/types"
	external "github.com/opensearch-project/opensearch-go/v2"
	api "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type openSearchClient struct {
	client        *external.Client
	index         string
	buff          []types.IndexableDocument
	flushInterval time.Duration
	buffSize      int
	m             sync.Mutex
}

func New(ctx context.Context, c *config.Config) (search.Searcher, error) {
	client, err := external.NewClient(external.Config{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		Addresses:  c.Opensearch.URLs,
		MaxRetries: c.Opensearch.MaxRetries,
		// Username:  c.Opensearch.Username,
		// Password:  c.Opensearch.Password, //disable security plugin = true in docker-compose.yaml
	})
	if err != nil {
		return nil, err
	}

	s := &openSearchClient{
		client:        client,
		index:         c.Opensearch.Index.Name,
		buff:          make([]types.IndexableDocument, 0, c.Opensearch.Index.BuffSize),
		flushInterval: time.Second * time.Duration(c.Opensearch.Index.FlushInterval),
		buffSize:      c.Opensearch.Index.BuffSize,
	}

	if err := s.checkAndCreateIndex(ctx, c.Opensearch.Index.Name); err != nil {
		return nil, err
	}

	go s.startFlushTicker(ctx)

	return s, nil
}

func (s *openSearchClient) checkAndCreateIndex(ctx context.Context, index string) error {
	if resp, err := s.client.Indices.Exists([]string{index}); err == nil {
		// early return if index already exists
		if resp.StatusCode == http.StatusOK {
			return nil
		}
	}

	settings := strings.NewReader(`{
        "settings": {
            "index": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }
    }`)

	req := api.IndicesCreateRequest{
		Index: index,
		Body:  settings,
	}

	resp, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("failed to create index: %s %s", resp.Status(), string(body))
	}

	if resp.HasWarnings() {
		log.Printf("[opensearch] warnings: %v", resp.Warnings())
	}

	log.Printf("[opensearch] index created: %s", index)

	return nil
}

func (s *openSearchClient) Search(ctx context.Context, query string) ([]search.Document, error) {
	// TODO: Implement search
	return nil, nil
}

func (s *openSearchClient) Index(ctx context.Context, doc types.IndexableDocument) error {
	s.m.Lock()
	defer s.m.Unlock()
	s.buff = append(s.buff, doc)
	return nil
}

func (s *openSearchClient) startFlushTicker(ctx context.Context) {
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.flush(ctx); err != nil {
				log.Printf("[opensearch] [flush] failed to flush documents: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *openSearchClient) flush(ctx context.Context) error {
	s.m.Lock()
	defer s.m.Unlock()

	if len(s.buff) == 0 {
		return nil
	}

	log.Printf("[opensearch] [flush] flushing %d documents", len(s.buff))

	bulkReq := strings.Builder{}
	for _, doc := range s.buff {
		if len(doc.Data) == 0 {
			log.Printf("[opensearch] [flush] empty document: %v", doc.Id)
			continue
		}
		jsonData, err := json.Marshal(doc.Data)
		if err != nil {
			log.Printf("[opensearch] [flush] failed to marshal document: %v", err)
			continue
		}
		bulkReq.WriteString(fmt.Sprintf(`{"index": {"_index": "%s", "_id": "%s"}}`, s.index, doc.Id))
		bulkReq.WriteString(fmt.Sprintf("%s\n", string(jsonData)))
	}

	reqBody := bulkReq.String()
	if reqBody == "" {
		return nil
	}

	req := api.BulkRequest{
		Body: strings.NewReader(reqBody),
	}

	resp, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("failed to flush documents: %s %s", resp.Status(), string(body))
	}

	log.Printf("[opensearch] [flush] flushed %d documents", len(s.buff))

	s.buff = make([]types.IndexableDocument, 0, s.buffSize)

	return nil
}

func (s *openSearchClient) DeIndex(ctx context.Context, id string) error {
	req := api.DeleteRequest{
		Index:      s.index,
		DocumentID: id,
	}

	resp, err := req.Do(ctx, s.client)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.IsError() {
		return fmt.Errorf("failed to delete document: %s %s", resp.Status(), string(body))
	}

	log.Printf("[opensearch] document deleted: %v", id)

	return nil
}
