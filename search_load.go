package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewSearchLoader(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, search func(ctx context.Context, m interface{}) (interface{}, int64, error)) (*Searcher, *Loader) {
	loader := NewLoader(db, tableName, modelType, partitionKeyName, sortKeyName)
	searcher := NewSearcher(search)
	return searcher, loader
}