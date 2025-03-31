package search

import (
	"context"
)

type Document map[string]any

type Searcher interface {
	Index(ctx context.Context, id string, data any) error
	DeIndex(ctx context.Context, id string) error
	Search(ctx context.Context, query string) ([]Document, error)
}
