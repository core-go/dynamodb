package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewSearchWriter(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, search func(ctx context.Context, m interface{}) (interface{}, int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}
