package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type Writer struct {
	*Loader
	maps         map[string]string
	versionField string
	versionIndex int
}

func NewWriter(db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, partitionKeyName string, sortKeyName string, versionFieldName string) *Writer {
	defaultViewService := NewLoader(db, tableName, modelType, partitionKeyName, sortKeyName)
	if len(versionFieldName) > 0 {
		if index, versionField, ok := GetFieldByName(modelType, versionFieldName); ok {
			return &Writer{Loader: defaultViewService, maps: MakeMapObject(modelType), versionField: versionField, versionIndex: index}
		}
	}
	return &Writer{Loader: defaultViewService, maps: MakeMapObject(modelType), versionField: "", versionIndex: -1}
}

func (m *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return InsertOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return InsertOne(ctx, m.Database, m.tableName, m.Keys(), model)
}

func (m *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return UpdateOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return UpdateOne(ctx, m.Database, m.tableName, m.Keys(), model)
}
func (m *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return PatchOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), MapToDBObject(model, m.maps), m.versionField)
	}
	return PatchOne(ctx, m.Database, m.tableName, m.Keys(), MapToDBObject(model, m.maps))
}

func (m *Writer) Save(ctx context.Context, model interface{}) (int64, error) {
	if m.versionIndex >= 0 {
		return UpsertOneWithVersion(ctx, m.Database, m.tableName, m.Keys(), model, m.versionIndex, m.versionField)
	}
	return UpsertOne(ctx, m.Database, m.tableName, model)
}

func (m *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	return DeleteOne(ctx, m.Database, m.tableName, m.Keys(), id)
}
