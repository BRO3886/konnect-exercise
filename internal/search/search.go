package search

import (
	"context"

	"github.com/BRO3886/konnect-exercise/internal/types"
)

type Document map[string]any

type Searcher interface {
	Index(ctx context.Context, doc types.IndexableDocument) error
	DeIndex(ctx context.Context, id string) error
	Search(ctx context.Context, query string) ([]Document, error)
}
