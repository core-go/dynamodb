package dynamodb

import (
	"context"
	"log"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type Loader struct {
	Database     *dynamodb.DynamoDB
	tableName    string
	modelType    reflect.Type
	partitionKey string
	sortKey      string
	Map          func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewLoader(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, options ...func(context.Context, interface{}) (interface{}, error)) *Loader {
	if len(partitionKeyName) == 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	_, partitionKey, ok := GetFieldByName(modelType, partitionKeyName)
	if !ok {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name())
	}
	_, sortKey, ok := GetFieldByName(modelType, sortKeyName)
	if !ok {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name())
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) > 0 && options[0] != nil {
		mp = options[0]
	}
	return &Loader{Database: db, tableName: tableName, modelType: modelType, partitionKey: partitionKey, sortKey: sortKey, Map: mp}
}

func (m *Loader) Keys() []string {
	if len(m.sortKey) != 0 {
		return []string{m.partitionKey, m.sortKey}
	}
	return []string{m.partitionKey}
}

func (m *Loader) All(ctx context.Context) (interface{}, error) {
	query, er1 := BuildQuery(m.tableName, SecondaryIndex{}, nil)
	if er1 != nil {
		return nil, er1
	}
	results, er2 := Find(ctx, m.Database, query, m.modelType)
	if er2 != nil {
		return results, er2
	}
	if results != nil && m.Map != nil {
		return MapModels(ctx, results, m.Map)
	}
	return results, nil
}

func (m *Loader) Load(ctx context.Context, id interface{}) (interface{}, error) {
	r, er1 := FindOne(ctx, m.Database, m.tableName, m.modelType, m.Keys(), id)
	if er1 != nil {
		return r, er1
	}
	if m.Map != nil {
		r2, er2 := m.Map(ctx, r)
		if er2 != nil {
			return r, er2
		}
		return r2, er2
	}
	return r, er1
}

func (m *Loader) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	ok, er1 := FindOneAndDecode(ctx, m.Database, m.tableName, m.Keys(), id, result)
	if ok && er1 == nil && m.Map != nil {
		_, er2 := m.Map(ctx, result)
		if er2 != nil {
			return ok, er2
		}
	}
	return ok, er1
}

func (m *Loader) Exist(ctx context.Context, id interface{}) (bool, error) {
	return Exist(ctx, m.Database, m.tableName, m.Keys(), id)
}
