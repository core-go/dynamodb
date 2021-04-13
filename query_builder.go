package dynamodb

import (
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/common-go/search"
)

type QueryBuilder struct {
	TableName string
	ModelType reflect.Type
	Index     SecondaryIndex
}

const (
	PREFIX  = "prefix"
	CONTAIN = "contain"
	EQUAL   = "equal"
)

func NewQueryBuilder(tableName string, resultModelType reflect.Type, index SecondaryIndex) *QueryBuilder {
	return &QueryBuilder{TableName: tableName, ModelType: resultModelType, Index: index}
}

func (b *QueryBuilder) BuildQuery(sm interface{}) (dynamodb.QueryInput, error) {
	query := dynamodb.QueryInput{}
	if _, ok := sm.(*search.SearchModel); ok {
		return query, nil
	}
	var conditionBuilders []*expression.ConditionBuilder
	var projectionBuilder *expression.ProjectionBuilder
	var keyword string
	value := reflect.Indirect(reflect.ValueOf(sm))
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		kind := field.Kind()
		x := field.Interface()
		ps := false
		var psv string
		if kind == reflect.Ptr {
			if field.IsNil() {
				continue
			}
			s0, ok0 := x.(*string)
			if ok0 {
				if s0 == nil || len(*s0) == 0 {
					continue
				}
				ps = true
				psv = *s0
			}
		}
		s0, ok0 := x.(string)
		if ok0 {
			if len(s0) == 0 {
				continue
			}
			psv = s0
		}
		if v, ok := x.(*search.SearchModel); ok {
			if len(v.Excluding) > 0 {
				for key, val := range v.Excluding {
					if _, name, ok := GetFieldByName(b.ModelType, key); ok {
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
		} else if ps || kind == reflect.String {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				var condition expression.ConditionBuilder
				if field.Len() > 0 {
					if key, ok := value.Type().Field(i).Tag.Lookup("match"); ok {
						if key == PREFIX {
							condition = expression.Name(name).BeginsWith(psv)
						} else if key == CONTAIN {
							condition = expression.Name(name).Contains(psv)
						} else if key == EQUAL {
							condition = expression.Name(name).Equal(expression.Value(psv))
						} else {
							log.Panicf("match not support \"%v\" format\n", key)
						}
					}
				} else if len(keyword) > 0 {
					if key, ok := value.Type().Field(i).Tag.Lookup("keyword"); ok {
						if key == PREFIX {
							condition = expression.Name(name).BeginsWith(psv)
						} else if key == CONTAIN {
							condition = expression.Name(name).Contains(psv)
						} else if key == EQUAL {
							condition = expression.Name(name).Equal(expression.Value(psv))
						} else {
							log.Panicf("match not support \"%v\" format\n", key)
						}
					}
				}
				conditionBuilders = append(conditionBuilders, &condition)
			}
		} else if rangeTime, ok := x.(*search.TimeRange); ok && rangeTime != nil {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				gte := expression.Name(name).GreaterThanEqual(expression.Value(rangeTime.StartTime))
				lt := expression.Name(name).LessThan(expression.Value(rangeTime.EndTime))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeTime, ok := x.(search.TimeRange); ok {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				gte := expression.Name(name).GreaterThanEqual(expression.Value(rangeTime.StartTime))
				lt := expression.Name(name).LessThan(expression.Value(rangeTime.EndTime))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeDate, ok := x.(*search.DateRange); ok && rangeDate != nil {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				startDate := rangeDate.StartDate
				endDate := rangeDate.EndDate.Add(time.Hour * 24)
				gte := expression.Name(name).GreaterThanEqual(expression.Value(startDate))
				lt := expression.Name(name).LessThan(expression.Value(endDate))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if rangeDate, ok := x.(search.DateRange); ok {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				startDate := rangeDate.StartDate
				endDate := rangeDate.EndDate.Add(time.Hour * 24)
				gte := expression.Name(name).GreaterThanEqual(expression.Value(startDate))
				lt := expression.Name(name).LessThan(expression.Value(endDate))
				c := gte.And(lt)
				conditionBuilders = append(conditionBuilders, &c)
			}
		} else if numberRange, ok := x.(*search.NumberRange); ok && numberRange != nil {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
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
		} else if numberRange, ok := x.(search.NumberRange); ok {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
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
		} else if kind == reflect.Slice {
			if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
				condition := expression.Name(name).In(expression.Value(x))
				conditionBuilders = append(conditionBuilders, &condition)
			}
		} else {
			t := kind.String()
			if _, ok := x.(*search.SearchModel); t == "bool" || (strings.Contains(t, "int") && x != 0) || (strings.Contains(t, "float") && x != 0) || (!ok && t == "string" && field.Len() > 0) || (!ok && t == "ptr" &&
				field.Pointer() != 0) {
				if _, name, ok := GetFieldByName(b.ModelType, value.Type().Field(i).Name); ok {
					c := expression.Not(expression.Name(name).Equal(expression.Value(x)))
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
	keyCondition, err := BuildKeyCondition(sm, b.Index, keyword)
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
		TableName:                 aws.String(b.TableName),
		IndexName:                 aws.String(b.Index.IndexName),
		ProjectionExpression:      expr.Projection(),
		KeyConditionExpression:    expr.KeyCondition(),
		FilterExpression:          expr.Filter(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		Select:                    aws.String(dynamodb.SelectSpecificAttributes),
	}
	return query, nil
}
