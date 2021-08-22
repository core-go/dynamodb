package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type SearchBuilder struct {
	DB         *dynamodb.DynamoDB
	ModelType  reflect.Type
	BuildQuery func(m interface{}) (dynamodb.ScanInput, error)
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSearchBuilder(db *dynamodb.DynamoDB, modelType reflect.Type, buildQuery func(interface{}) (dynamodb.ScanInput, error), options ...func(context.Context, interface{}) (interface{}, error)) *SearchBuilder {
	var mp func(ctx context.Context, model interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return &SearchBuilder{DB: db, ModelType: modelType, BuildQuery: buildQuery, Map: mp}
}
func (b *SearchBuilder) Search(ctx context.Context, m interface{}, results interface{}, limit int64, options ...int64) (int64, string, error) {
	query, er1 := b.BuildQuery(m)
	if er1 != nil {
		return 0, "", er1
	}
	var skip int64 = 0
	if len(options) > 0 && options[0] > 0 {
		skip = options[0]
	}
	if skip == 0 {
		total, er2 := BuildSearchResult(ctx, b.DB, results, query, limit, 1, b.Map)
		return total, "", er2
	} else {
		pageIndex := skip / limit
		m := skip % limit
		if m > 0 {
			pageIndex = pageIndex + 1
		}
		total, er2 := BuildSearchResult(ctx, b.DB, results, query, limit, pageIndex, b.Map)
		return total, "", er2
	}
}
