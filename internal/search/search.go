package search

import (
	"context"
)

type Document map[string]any

type Searcher interface {
	Index(ctx context.Context, index string, id string, data any) error
	DeIndex(ctx context.Context, index string, id string) error
	Search(ctx context.Context, index string, query string) ([]Document, error)
}
