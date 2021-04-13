package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type SearchBuilder struct {
	DB         *dynamodb.DynamoDB
	ModelType  reflect.Type
	BuildQuery func(m interface{}) (dynamodb.QueryInput, error)
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSearchBuilder(db *dynamodb.DynamoDB, modelType reflect.Type, buildQuery func(interface{}) (dynamodb.QueryInput, error), options...func(context.Context, interface{}) (interface{}, error)) *SearchBuilder {
	var mp func(ctx context.Context, model interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return &SearchBuilder{DB: db, ModelType: modelType, BuildQuery: buildQuery, Map: mp}
}
func (b *SearchBuilder) Search(ctx context.Context, m interface{}, results interface{}, pageIndex int64, pageSize int64, options ...int64) (int64, error) {
	query, er1 := b.BuildQuery(m)
	if er1 != nil {
		return 0, er1
	}
	var firstPageSize int64
	if len(options) > 0 && options[0] > 0 {
		firstPageSize = options[0]
	} else {
		firstPageSize = 0
	}
	return BuildSearchResult(ctx, b.DB, results, query, pageIndex, pageSize, firstPageSize, b.Map)
}
