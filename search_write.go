package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewSearchWriter(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, buildQuery func(interface{}) (dynamodb.ScanInput, error), options ...Mapper) (*Searcher, *Writer) {
	return NewSearchWriterWithVersionAndQuery(db, tableName, modelType, partitionKeyName, sortKeyName, "", buildQuery, options...)
}
func NewSearchWriterWithVersionAndQuery(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, buildQuery func(interface{}) (dynamodb.ScanInput, error), options ...Mapper) (*Searcher, *Writer) {
	var mapper Mapper
	if len(options) > 0 && options[0] != nil {
		mapper = options[0]
	}
	if mapper != nil {
		writer := NewWriterWithVersion(db, tableName, modelType, partitionKeyName, sortKeyName, versionField, options...)
		searcher := NewSearcherWithQuery(db, modelType, buildQuery, mapper.DbToModel)
		return searcher, writer
	} else {
		writer := NewWriterWithVersion(db, tableName, modelType, partitionKeyName, sortKeyName, versionField, options...)
		searcher := NewSearcherWithQuery(db, modelType, buildQuery)
		return searcher, writer
	}
}
func NewSearchWriterWithVersion(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionField string, search func(context.Context, interface{}, interface{}, int64, ...int64) (int64, string, error)) (*Searcher, *Writer) {
	writer := NewWriterWithVersion(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searcher := NewSearcher(search)
	return searcher, writer
}
