package dynamodb

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

type DynamodbHealthChecker struct {
	db      *dynamodb.DynamoDB
	name    string
	timeout time.Duration
}

func NewDynamodbHealthCheckerWithTimeout(db *dynamodb.DynamoDB, name string, timeout time.Duration) *DynamodbHealthChecker {
	return &DynamodbHealthChecker{db, name, timeout}
}
func NewDynamodbHealthChecker(db *dynamodb.DynamoDB, options ...string) *DynamodbHealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "dynamodb"
	}
	return NewDynamodbHealthCheckerWithTimeout(db, name, 4 * time.Second)
}

func (s *DynamodbHealthChecker) Name() string {
	return s.name
}

func (s *DynamodbHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{}, 0)
	if s.timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, s.timeout)
	}

	checkerChan := make(chan error)
	go func() {
		input := &dynamodb.ListTablesInput{}
		_, err := s.db.ListTables(input)
		checkerChan <- err
	}()
	select {
	case err := <-checkerChan:
		if err != nil {
			return res, err
		}
		res["status"] = "success"
		return res, err
	case <-ctx.Done():
		return nil, errors.New("connection timout")
	}
}

func (s *DynamodbHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
