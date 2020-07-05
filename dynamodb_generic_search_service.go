package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewGenericSearchService(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, searchBuilder SearchResultBuilder, partitionKeyName string, sortKeyName string, versionField string) (*GenericService, *SearchService) {
	genericService := NewGenericService(db, tableName, modelType, partitionKeyName, sortKeyName, versionField)
	searchService := NewSearchService(db, tableName, modelType, searchBuilder)
	return genericService, searchService
}
