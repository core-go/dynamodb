package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

func NewViewSearchService(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, searchBuilder SearchResultBuilder, partitionKeyName string, sortKeyName string) (*ViewService, *SearchService) {
	viewService := NewViewService(db, tableName, modelType, partitionKeyName, sortKeyName)
	searchService := NewSearchService(db, tableName, modelType, searchBuilder)
	return viewService, searchService
}
