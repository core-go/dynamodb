package dynamodb

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDBHealthChecker struct {
	db      *dynamodb.DynamoDB
	name    string
	timeout time.Duration
}

func NewDynamoDBHealthChecker(db *dynamodb.DynamoDB, name string, timeouts ...time.Duration) *DynamoDBHealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &DynamoDBHealthChecker{db, name, timeout}
}
func NewHealthChecker(db *dynamodb.DynamoDB, options ...string) *DynamoDBHealthChecker {
	var name string
	if len(options) > 0 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "dynamodb"
	}
	return NewDynamoDBHealthChecker(db, name, 4 * time.Second)
}

func (s *DynamoDBHealthChecker) Name() string {
	return s.name
}

func (s *DynamoDBHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
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

func (s *DynamoDBHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
