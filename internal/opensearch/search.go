package opensearch

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/BRO3886/konnect-exercise/internal/config"
	"github.com/BRO3886/konnect-exercise/internal/search"
	external "github.com/opensearch-project/opensearch-go/v2"
	api "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
)

type openSearchClient struct {
	client *external.Client
	index  string
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
		client: client,
		index:  c.Opensearch.Index.Name,
	}

	if err := s.checkAndCreateIndex(ctx, c.Opensearch.Index.Name); err != nil {
		return nil, err
	}

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

func (s *openSearchClient) Index(ctx context.Context, id string, data any) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	req := api.IndexRequest{
		Index:      s.index,
		DocumentID: id,
		Body:       bytes.NewReader(jsonData),
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
		return fmt.Errorf("failed to index document: %s %s", resp.Status(), string(body))
	}

	if resp.HasWarnings() {
		log.Printf("[opensearch] warnings: %v", resp.Warnings())
	}

	log.Printf("[opensearch] document indexed: %v", data)

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
