package dynamodb

import (
	"context"
)
import "github.com/aws/aws-sdk-go/aws"
import "github.com/aws/aws-sdk-go/service/dynamodb"
import "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

type Inserter struct {
	DB *dynamodb.DynamoDB
	tableName string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewInserter(database *dynamodb.DynamoDB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return &Inserter{DB: database, tableName: tableName, Map: mp}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
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
	avModel, err := dynamodbattribute.MarshalMap(modelNew)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      avModel,
		TableName: aws.String(w.tableName),
	}
	_, err = w.DB.PutItem(input)
	return err
}