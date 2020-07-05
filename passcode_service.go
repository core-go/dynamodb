package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

type PasscodeService struct {
	Database      *dynamodb.DynamoDB
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeService(db *dynamodb.DynamoDB, tableName, keyName, passcodeName, expiredAtName string) *PasscodeService {
	return &PasscodeService{db, tableName, keyName, passcodeName, expiredAtName}
}

func NewDefaultPasscodeService(db *dynamodb.DynamoDB, tableName string) *PasscodeService {
	return NewPasscodeService(db, tableName, "_id", "passcode", "expiredAt")
}

func (s *PasscodeService) Save(ctx context.Context, id string, passcode string, expiredAt time.Time) (int64, error) {
	pass := make(map[string]interface{})
	pass[s.idName] = id
	pass[s.passcodeName] = passcode
	pass[s.expiredAtName] = expiredAt
	return UpsertOne(ctx, s.Database, s.tableName, pass)
}

func (s *PasscodeService) Load(ctx context.Context, id string) (string, time.Time, error) {
	ok, data, err := FindOneAndReturnMapData(ctx, s.Database, s.tableName, []string{s.idName}, id)
	if err != nil || !ok {
		return "", time.Now(), err
	}
	return data[s.passcodeName].(string), data[s.passcodeName].(time.Time), nil
}

func (s *PasscodeService) Delete(ctx context.Context, id string) (int64, error) {
	return DeleteOne(ctx, s.Database, s.tableName, []string{s.idName}, id)
}
