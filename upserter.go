package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
	"github.com/common-go/mongo"
)

type  Upserter struct {
	database   *dynamodb.DynamoDB
	tableName  string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpserterById(database *dynamodb.DynamoDB, tableName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Upserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	if len(fieldName) == 0 {
		_, idName := mongo.FindIdField(modelType)
		fieldName = idName
	}
	return &Upserter{tableName: tableName, database: database, Map: mp}
}

func NewUpserter(database *dynamodb.DynamoDB, collectionName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Upserter {
	return NewUpserterById(database, collectionName, modelType, "", options...)
}

func (w *Upserter) Write(ctx context.Context, model interface{}) error {
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
	_, err = UpsertOne(ctx, w.database, w.tableName, modelNew)
	return err
}