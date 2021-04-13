package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func NewSearchWriterWithQuery(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, buildQuery func(interface{}) (dynamodb.QueryInput, error), options...func(context.Context, interface{}) (interface{}, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searcher := NewSearcherWithQuery(db, modelType, buildQuery, options...)
	return searcher, writer
}

func NewSearchWriter(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}
