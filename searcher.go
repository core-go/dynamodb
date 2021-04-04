package dynamodb

import (
	"context"
)

type Searcher struct {
	search func(ctx context.Context, m interface{}) (interface{}, int64, error)
}

func NewSearcher(search func(ctx context.Context, m interface{}) (interface{}, int64, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}) (interface{}, int64, error) {
	return s.search(ctx, m)
}