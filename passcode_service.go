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

func NewPasscodeService(db *dynamodb.DynamoDB, tableName string, options ...string) *PasscodeService {
	var keyName, passcodeName, expiredAtName string
	if len(options) >= 1 && len(options[0]) > 0 {
		expiredAtName = options[0]
	} else {
		expiredAtName = "expiredAt"
	}
	if len(options) >= 2 && len(options[1]) > 0 {
		keyName = options[1]
	} else {
		keyName = "id"
	}
	if len(options) >= 3 && len(options[2]) > 0 {
		passcodeName = options[2]
	} else {
		passcodeName = "passcode"
	}
	return &PasscodeService{db, tableName, keyName, passcodeName, expiredAtName}
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
