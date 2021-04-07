package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"github.com/common-go/mongo"
)

type Updater struct {
	writer 	   *Writer
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpdaterById(database *dynamodb.DynamoDB, tableName string,  modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	if len(fieldName) == 0 {
		_, idName := mongo.FindIdField(modelType)
		fieldName = idName
	}
	return &Updater{Map: mp, writer: NewWriter(database, tableName, modelType, fieldName, "", "")}
}

func NewUpdater(database *dynamodb.DynamoDB, tableName string,  modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Updater {
	return NewUpdaterById(database, tableName, modelType, "", options...)
}

func (w *Updater) Write(ctx context.Context, model interface{}) error {
	var modelNew interface{}
	var err error
	if w.Map != nil {
		modelNew, err = w.Map(ctx, model)
		if err != nil {
			return err
		}
	} else {
		modelNew = model
	}
	_, error := w.writer.Update(ctx, modelNew)
	return error
}
