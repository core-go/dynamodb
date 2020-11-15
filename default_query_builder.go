package dynamodb

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/common-go/search"
	"log"
	"reflect"
	"strings"
	"time"
)

type DefaultQueryBuilder struct {
}

const (
	PREFIX  = "prefix"
	CONTAIN = "contain"
)

func (b *DefaultQueryBuilder) BuildQuery(sm interface{}, resultModelType reflect.Type, tableName string, index SecondaryIndex) (dynamodb.QueryInput, error) {
	query := dynamodb.QueryInput{}
	if _, ok := sm.(*search.SearchModel); ok {
		return query, nil
	}
	var conditionBuilders []*expression.ConditionBuilder
	var projectionBuilder *expression.ProjectionBuilder
	var keyword string
	objectValue := reflect.Indirect(reflect.ValueOf(sm))
	for i := 0; i < objectValue.NumField(); i++ {
		fieldValue := objectValue.Field(i).Interface()
		if v, ok := fieldValue.(*search.SearchModel); ok {
			if len(v.Excluding) > 0 {
				for key, val := range v.Excluding {
					if _, name, ok := GetFieldByName(resultModelType, key); ok {
						if len(val) > 0 {
							c := expression.Not(expression.Name(name).In(expression.Value(val)))
							conditionBuilders = append(conditionBuilders, &c)
						}
					}
				}
			} else if len(v.Keyword) > 0 {
				keyword = strings.TrimSpace(v.Keyword)
			}
			if len(v.Fields) > 0 {
				for idx := range v.Fields {
					projection := expression.NamesList(expression.Name(v.Fields[idx]))
					projectionBuilder = &projection
				}
			}
			continue
		} else if rangeTime, ok := fieldValue.(*search.TimeRange); ok && rangeTime != nil {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				gte := expression.Name(name).GreaterThanEqual(expression.Value(rangeTime.StartTime))
				lt := expression.Name(name).LessThan(expression.Value(rangeTime.EndTime))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeTime, ok := fieldValue.(search.TimeRange); ok {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				gte := expression.Name(name).GreaterThanEqual(expression.Value(rangeTime.StartTime))
				lt := expression.Name(name).LessThan(expression.Value(rangeTime.EndTime))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeDate, ok := fieldValue.(*search.DateRange); ok && rangeDate != nil {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				startDate := rangeDate.StartDate
				endDate := rangeDate.EndDate.Add(time.Hour * 24)
				gte := expression.Name(name).GreaterThanEqual(expression.Value(startDate))
				lt := expression.Name(name).LessThan(expression.Value(endDate))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeDate, ok := fieldValue.(search.DateRange); ok {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				startDate := rangeDate.StartDate
				endDate := rangeDate.EndDate.Add(time.Hour * 24)
				gte := expression.Name(name).GreaterThanEqual(expression.Value(startDate))
				lt := expression.Name(name).LessThan(expression.Value(endDate))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if numberRange, ok := fieldValue.(*search.NumberRange); ok && numberRange != nil {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				var arr []*expression.ConditionBuilder
				if numberRange.Min != nil {
					gte := expression.Name(name).GreaterThanEqual(expression.Value(numberRange.Min))
					arr = append(arr, &gte)
				} else if numberRange.Lower != nil {
					gt := expression.Name(name).GreaterThan(expression.Value(numberRange.Lower))
					arr = append(arr, &gt)
				}
				if numberRange.Max != nil {
					lte := expression.Name(name).LessThanEqual(expression.Value(numberRange.Max))
					arr = append(arr, &lte)
				} else if numberRange.Upper != nil {
					lt := expression.Name(name).LessThan(expression.Value(numberRange.Upper))
					arr = append(arr, &lt)
				}

				var f *expression.ConditionBuilder
				for idx := range arr {
					if f == nil {
						f = arr[idx]
					} else {
						f.And(*arr[idx])
					}
				}
				conditionBuilders = append(conditionBuilders, f)
			}
		} else if numberRange, ok := fieldValue.(search.NumberRange); ok {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				var arr []*expression.ConditionBuilder
				if numberRange.Min != nil {
					gte := expression.Name(name).GreaterThanEqual(expression.Value(numberRange.Min))
					arr = append(arr, &gte)
				} else if numberRange.Lower != nil {
					gt := expression.Name(name).GreaterThan(expression.Value(numberRange.Lower))
					arr = append(arr, &gt)
				}
				if numberRange.Max != nil {
					lte := expression.Name(name).LessThanEqual(expression.Value(numberRange.Max))
					arr = append(arr, &lte)
				} else if numberRange.Upper != nil {
					lt := expression.Name(name).LessThan(expression.Value(numberRange.Upper))
					arr = append(arr, &lt)
				}

				var f *expression.ConditionBuilder
				for idx := range arr {
					if f == nil {
						f = arr[idx]
					} else {
						f.And(*arr[idx])
					}
				}
				conditionBuilders = append(conditionBuilders, f)
			}
		} else if objectValue.Field(i).Kind() == reflect.Slice {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				condition := expression.Name(name).In(expression.Value(fieldValue))
				conditionBuilders = append(conditionBuilders, &condition)
			}
		} else if objectValue.Field(i).Kind() == reflect.String {
			if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
				var condition expression.ConditionBuilder
				if objectValue.Field(i).Len() > 0 {
					if key, ok := objectValue.Type().Field(i).Tag.Lookup("match"); ok {
						if key == PREFIX {
							condition = expression.Name(name).BeginsWith(objectValue.Field(i).String())
						} else if key == CONTAIN {
							condition = expression.Name(name).Contains(objectValue.Field(i).String())
						} else {
							log.Panicf("match not support \"%v\" format\n", key)
						}
					}
				} else if len(keyword) > 0 {
					if key, ok := objectValue.Type().Field(i).Tag.Lookup("keyword"); ok {
						if key == PREFIX {
							condition = expression.Name(name).BeginsWith(objectValue.Field(i).String())

						} else if key == CONTAIN {
							condition = expression.Name(name).Contains(objectValue.Field(i).String())
						} else {
							log.Panicf("match not support \"%v\" format\n", key)
						}
					}
				}
				conditionBuilders = append(conditionBuilders, &condition)
			}
		} else {
			t := objectValue.Field(i).Kind().String()
			if _, ok := fieldValue.(*search.SearchModel); t == "bool" || (strings.Contains(t, "int") && fieldValue != 0) || (strings.Contains(t, "float") && fieldValue != 0) || (!ok && t == "string" && objectValue.Field(i).Len() > 0) || (!ok && t == "ptr" &&
				objectValue.Field(i).Pointer() != 0) {
				if _, name, ok := GetFieldByName(resultModelType, objectValue.Type().Field(i).Name); ok {
					c := expression.Not(expression.Name(name).Equal(expression.Value(fieldValue)))
					conditionBuilders = append(conditionBuilders, &c)
				}
			}
		}
	}
	var filter *expression.ConditionBuilder
	for idx := range conditionBuilders {
		if filter == nil {
			filter = conditionBuilders[idx]
		} else {
			filter.And(*conditionBuilders[idx])
		}
	}
	keyCondition, err := BuildKeyCondition(sm, index, keyword)
	if err != nil {
		return query, err
	}
	builder := expression.NewBuilder().WithKeyCondition(keyCondition)
	if projectionBuilder != nil {
		builder.WithProjection(*projectionBuilder)
	}
	if filter != nil {
		builder.WithFilter(*filter)
	}
	expr, err := builder.Build()
	if err != nil {
		return query, err
	}
	query = dynamodb.QueryInput{
		TableName:                 aws.String(tableName),
		IndexName:                 aws.String(index.IndexName),
		ProjectionExpression:      expr.Projection(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Select:                    aws.String(dynamodb.SelectSpecificAttributes),
	}
	return query, nil
}

func ExtractSearchInfo(m interface{}) (string, int64, int64, int64, error) {
	if sModel, ok := m.(*search.SearchModel); ok {
		return sModel.Sort, sModel.PageIndex, sModel.PageSize, sModel.FirstPageSize, nil
	} else {
		value := reflect.Indirect(reflect.ValueOf(m))
		numField := value.NumField()
		for i := 0; i < numField; i++ {
			if sModel1, ok := value.Field(i).Interface().(*search.SearchModel); ok {
				return sModel1.Sort, sModel1.PageIndex, sModel1.PageSize, sModel1.FirstPageSize, nil
			}
		}
		return "", 0, 0, 0, errors.New("cannot extract sort, pageIndex, pageSize, firstPageSize from model")
	}
}
