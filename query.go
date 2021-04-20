package dynamodb

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	//"github.com/mitchellh/mapstructure"

)

func BuildSearchResult(ctx context.Context, db *dynamodb.DynamoDB, results interface{}, query dynamodb.ScanInput, pageIndex int64, pageSize int64, initPageSize int64, options...func(context.Context, interface{}) (interface{}, error)) (int64, error) {
	var databaseQuery *dynamodb.ScanOutput
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	if initPageSize > 0 && pageIndex == 1 {
		query.SetLimit(initPageSize)
	} else if pageSize > 0  {
		query.SetLimit(pageSize)
	}
	pageNum := 0
	err := db.ScanPagesWithContext(ctx, &query,
		func(page *dynamodb.ScanOutput, lastPage bool) bool {
			pageNum++
			if pageNum == int(pageIndex) {
				databaseQuery = page
			}
			return pageNum <= int(pageIndex)
		})
	if err != nil {
		return 0, err
	}

	err = dynamodbattribute.UnmarshalListOfMaps(databaseQuery.Items, &results)
	if err != nil {
		return 0, err
	}
	count := *databaseQuery.Count
	if mp == nil {
		return count, nil
	}
	_, er3 := MapModels(ctx, results, mp)
	return count, er3
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

func MapModels(ctx context.Context, models interface{}, mp func(context.Context, interface{}) (interface{}, error)) (interface{}, error) {
	valueModelObject := reflect.Indirect(reflect.ValueOf(models))
	if valueModelObject.Kind() == reflect.Ptr {
		valueModelObject = reflect.Indirect(valueModelObject)
	}
	if valueModelObject.Kind() == reflect.Slice {
		le := valueModelObject.Len()
		for i := 0; i < le; i++ {
			x := valueModelObject.Index(i)
			k := x.Kind()
			if k == reflect.Struct {
				y := x.Addr().Interface()
				mp(ctx, y)
			} else  {
				y := x.Interface()
				mp(ctx, y)
			}

		}
	}
	return models, nil
}
