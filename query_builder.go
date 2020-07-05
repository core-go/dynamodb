package dynamodb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"reflect"
)

type QueryBuilder interface {
	BuildQuery(sm interface{}, resultModelType reflect.Type, tableName string, index SecondaryIndex) (dynamodb.QueryInput, error)
}
