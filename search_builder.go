package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"reflect"
	"strings"
)

type SearchBuilder struct {
	DB *dynamodb.DynamoDB
	ModelType reflect.Type
	BuildQuery        func(m interface{}) (dynamodb.QueryInput, error)
	ExtractSearchInfo func(m interface{}) (string, int64, int64, int64, error)
}

func NewSearchBuilder(db *dynamodb.DynamoDB, modelType reflect.Type, buildQuery func(interface{}) (dynamodb.QueryInput, error), extract func(m interface{}) (string, int64, int64, int64, error)) *SearchBuilder {
	return &SearchBuilder{DB: db, ModelType: modelType, BuildQuery: buildQuery, ExtractSearchInfo: extract}
}
func (b *SearchBuilder) Search(ctx context.Context, m interface{}) (interface{}, int64, error) {
	query, er1 := b.BuildQuery(m)
	if er1 != nil {
		return nil, 0, er1
	}
	_, pageIndex, pageSize, firstPageSize, er2 := b.ExtractSearchInfo(m)
	if er2 != nil {
		return nil, 0, er2
	}
	return BuildSearchResult(ctx, b.DB, b.ModelType, query, pageIndex, pageSize, firstPageSize)
}

func BuildSearchResult(ctx context.Context, db *dynamodb.DynamoDB, modelType reflect.Type, query dynamodb.QueryInput, pageIndex int64, pageSize int64, initPageSize int64) (interface{}, int64, error) {
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
		return nil, 0, err
	}

	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	results := reflect.New(modelsType).Interface()
	err = dynamodbattribute.UnmarshalListOfMaps(databaseQuery.Items, results)
	if err != nil {
		return nil, 0, err
	}
	count := *databaseQuery.Count
	return results, count, nil
}

func BuildKeyCondition(sm interface{}, index SecondaryIndex, keyword string) (expression.KeyConditionBuilder, error) {
	var keyCondition *expression.KeyConditionBuilder
	var keyConditionBuilders []*expression.KeyConditionBuilder
	objectValue := reflect.Indirect(reflect.ValueOf(sm))
	objectModel := objectValue.Type()
	for _, key := range index.Keys {
		if i, _, ok := GetFieldByTagName(objectModel, key); ok {
			fieldValue := reflect.Indirect(objectValue.Field(i))
			if fieldValue.Kind() == reflect.String {
				var builder expression.KeyConditionBuilder
				if fieldValue.Len() > 0 {
					if key, ok := objectValue.Type().Field(i).Tag.Lookup("match"); ok {
						if key == PREFIX {
							builder = expression.Key(key).BeginsWith(fieldValue.String())
						} else {
							return *keyCondition, fmt.Errorf("match not support \"%v\" format\n", key)
						}
					}
				} else if len(keyword) > 0 {
					if key, ok := objectValue.Type().Field(i).Tag.Lookup("keyword"); ok {
						if key == PREFIX {
							builder = expression.Key(key).BeginsWith(fieldValue.String())
						} else {
							return *keyCondition, fmt.Errorf("match not support \"%v\" format\n", key)
						}
					}
				}
				keyConditionBuilders = append(keyConditionBuilders, &builder)
			} else {
				t := fieldValue.Kind().String()
				if (strings.Contains(t, "int") && fieldValue.Int() != 0) || (strings.Contains(t, "float") && fieldValue.Float() != 0) {
					builder := expression.Key(key).Equal(expression.Value(fieldValue.Interface()))
					keyConditionBuilders = append(keyConditionBuilders, &builder)
				} else {
					return *keyCondition, fmt.Errorf("key condition not support \"%v\" type\n", key)
				}
			}

		}
	}
	for idx := range keyConditionBuilders {
		if keyCondition == nil {
			keyCondition = keyConditionBuilders[idx]
		} else {
			keyCondition.And(*keyConditionBuilders[idx])
		}
	}
	return *keyCondition, nil
}
