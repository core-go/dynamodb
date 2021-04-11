package dynamodb

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func NewSearchWriter(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, search func(ctx context.Context, m interface{}) (interface{}, int64, error)) (*Searcher, *Writer) {
	writer := NewWriter(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}
