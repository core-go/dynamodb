package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type SearchResultBuilder interface {
	BuildSearchResult(ctx context.Context, db *dynamodb.DynamoDB, searchModel interface{}, modelType reflect.Type, tableName string) (interface{}, int64, error)
}
