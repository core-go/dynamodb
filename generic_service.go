package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type GenericService struct {
	*ViewService
	maps         map[string]string
	versionField string
	versionIndex int
}

func NewGenericService(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionFieldName string) *GenericService {
	defaultViewService := NewViewService(db, tableName, modelType, partitionKeyName, sortKeyName)
	if len(versionFieldName) > 0 {
		if index, versionField, ok := GetFieldByName(modelType, versionFieldName); ok {
			return &GenericService{ViewService: defaultViewService, maps: MakeMapObject(modelType), versionField: versionField, versionIndex: index}
		}
	}
	return &GenericService{ViewService: defaultViewService, maps: MakeMapObject(modelType), versionField: "", versionIndex: -1}
}

func (m *GenericService) Insert(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return InsertOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return InsertOne(ctx, m.Database, m.tableName, m.Keys(), model)
}

func (m *GenericService) Update(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return UpdateOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return UpdateOne(ctx, m.Database, m.tableName, m.Keys(), model)
}
func (m *GenericService) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return PatchOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), MapToDBObject(model, m.maps), m.versionField)
	}
	return PatchOne(ctx, m.Database, m.tableName, m.Keys(), MapToDBObject(model, m.maps))
}

func (m *GenericService) Save(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return UpsertOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return UpsertOne(ctx, m.Database, m.tableName, model)
}

func (m *GenericService) Delete(ctx context.Context, id interface{}) (int64, error) {
	return DeleteOne(ctx, m.Database, m.tableName, m.Keys(), id)
}
