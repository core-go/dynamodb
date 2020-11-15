package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type SearchService struct {
	Database      *dynamodb.DynamoDB
	tableName     string
	modelType     reflect.Type
	searchBuilder SearchResultBuilder
}

func NewSearchService(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, searchBuilder SearchResultBuilder) *SearchService {
	return &SearchService{db, tableName, modelType, searchBuilder}
}

func (s *SearchService) Search(ctx context.Context, m interface{}) (interface{}, int64, error) {
	return s.searchBuilder.BuildSearchResult(ctx, s.Database, m, s.modelType, s.tableName)
}
