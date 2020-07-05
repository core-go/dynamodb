package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/common-go/search"
	"reflect"
)

type SearchResultBuilder interface {
	BuildSearchResult(ctx context.Context, db *dynamodb.DynamoDB, searchModel interface{}, modelType reflect.Type, tableName string) (*search.SearchResult, error)
}
