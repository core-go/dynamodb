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
}

func NewLoader(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string) *Loader {
	if len(partitionKeyName) == 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	_, partitionKey, ok := GetFieldByName(modelType, partitionKeyName)
	if !ok {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name())
	}
	_, sortKey, ok := GetFieldByName(modelType, sortKeyName)
	if !ok {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name())
	}
	return &Loader{db, tableName, modelType, partitionKey, sortKey}
}

func (m *Loader) Keys() []string {
	if len(m.sortKey) != 0 {
		return []string{m.partitionKey, m.sortKey}
	}
	return []string{m.partitionKey}
}

func (m *Loader) All(ctx context.Context) (interface{}, error) {
	query, err := BuildQuery(m.tableName, SecondaryIndex{}, nil)
	if err != nil {
		return nil, err
	}
	return Find(ctx, m.Database, query, m.modelType)
}

func (m *Loader) Load(ctx context.Context, id interface{}) (interface{}, error) {
	return FindOne(ctx, m.Database, m.tableName, m.modelType, m.Keys(), id)
}

func (m *Loader) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	return FindOneAndDecode(ctx, m.Database, m.tableName, m.Keys(), id, result)
}

func (m *Loader) Exist(ctx context.Context, id interface{}) (bool, error) {
	return Exist(ctx, m.Database, m.tableName, m.Keys(), id)
}
