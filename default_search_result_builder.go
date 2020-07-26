package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/common-go/search"
	"reflect"
)

type DefaultSearchResultBuilder struct {
	QueryBuilder QueryBuilder
}

func (b *DefaultSearchResultBuilder) BuildSearchResult(ctx context.Context, db *dynamodb.DynamoDB, m interface{}, modelType reflect.Type, tableName string, index SecondaryIndex) (*search.SearchResult, error) {
	query, err := b.QueryBuilder.BuildQuery(m, modelType, tableName, index)
	if err != nil {
		return nil, err
	}
	var searchModel *search.SearchModel

	if sModel, ok := m.(*search.SearchModel); ok {
		searchModel = sModel
	} else {
		value := reflect.Indirect(reflect.ValueOf(m))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*search.SearchModel); ok {
				searchModel = sModel1
			}
		}
	}
	return b.Build(ctx, db, modelType, query, searchModel.Page, searchModel.Limit, searchModel.FirstLimit)
}

func (b *DefaultSearchResultBuilder) Build(ctx context.Context, db *dynamodb.DynamoDB, modelType reflect.Type, query dynamodb.QueryInput, pageIndex int64, pageSize int64, initPageSize int64) (*search.SearchResult, error) {
	var databaseQuery *dynamodb.QueryOutput
	if initPageSize > 0 && pageIndex == 1 {
		query.SetLimit(initPageSize)
	} else {
		query.SetLimit(pageSize)
	}
	pageNum := 0
	err := db.QueryPagesWithContext(ctx, &query,
		func(page *dynamodb.QueryOutput, lastPage bool) bool {
			pageNum++
			if pageNum == int(pageIndex) {
				databaseQuery = page
			}
			return pageNum <= int(pageIndex)
		})
	if err != nil {
		return nil, err
	}

	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	results := reflect.New(modelsType).Interface()
	err = dynamodbattribute.UnmarshalListOfMaps(databaseQuery.Items, results)
	if err != nil {
		return nil, err
	}
	count := *databaseQuery.Count
	searchResult := search.SearchResult{}
	searchResult.Total = count
	searchResult.Last = false
	lengthModels := int64(reflect.Indirect(reflect.ValueOf(results)).Len())
	var receivedItems int64
	if initPageSize > 0 {
		if pageIndex == 1 {
			receivedItems = initPageSize
		} else if pageIndex > 1 {
			receivedItems = pageSize*(pageIndex-2) + initPageSize + lengthModels
		}
	} else {
		receivedItems = pageSize*(pageIndex-1) + lengthModels
	}
	searchResult.Last = receivedItems >= count
	searchResult.Results = results
	return &searchResult, nil
}
