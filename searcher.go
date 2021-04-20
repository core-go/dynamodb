package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Searcher struct {
	search func(ctx context.Context, searchModel interface{}, results interface{}, pageIndex int64, pageSize int64, options...int64) (int64, error)
}

func NewSearcherWithQuery(db *dynamodb.DynamoDB, modelType reflect.Type, buildQuery func(interface{}) (dynamodb.ScanInput, error), options...func(context.Context, interface{}) (interface{}, error)) *Searcher {
	builder := NewSearchBuilder(db, modelType, buildQuery, options...)
	return NewSearcher(builder.Search)
}
func NewSearcher(search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, pageIndex int64, pageSize int64, options...int64) (int64, error) {
	return s.search(ctx, m, results, pageIndex, pageSize, options...)
}
