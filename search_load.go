package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewSearchLoaderWithQuery(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, buildQuery func(interface{}) (dynamodb.ScanInput, error), options...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Loader) {
	loader := NewLoader(db, tableName, modelType, partitionKeyName, sortKeyName)
	searcher := NewSearcherWithQuery(db, modelType, buildQuery, options...)
	return searcher, loader
}

func NewSearchLoader(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)) (*Searcher, *Loader) {
	loader := NewLoader(db, tableName, modelType, partitionKeyName, sortKeyName)
	searcher := NewSearcher(search)
	return searcher, loader
}
