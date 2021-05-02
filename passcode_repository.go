package dynamodb

import (
	"context"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

type PasscodeRepository struct {
	Database      *dynamodb.DynamoDB
	tableName     string
	idName        string
	passcodeName  string
	expiredAtName string
}

func NewPasscodeRepository(db *dynamodb.DynamoDB, tableName string, options ...string) *PasscodeRepository {
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
	return &PasscodeRepository{db, tableName, keyName, passcodeName, expiredAtName}
}

func (p *PasscodeRepository) Save(ctx context.Context, id string, passcode string, expiredAt time.Time) (int64, error) {
	pass := make(map[string]interface{})
	pass[p.idName] = id
	pass[p.passcodeName] = passcode
	pass[p.expiredAtName] = expiredAt
	return UpsertOne(ctx, p.Database, p.tableName, []string{p.idName}, pass)
}

func (p *PasscodeRepository) Load(ctx context.Context, id string) (string, time.Time, error) {
	ok, data, err := FindOneAndReturnMapData(ctx, p.Database, p.tableName, []string{p.idName}, id)
	if err != nil || !ok {
		return "", time.Now(), err
	}
	return data[p.passcodeName].(string), data[p.passcodeName].(time.Time), nil
}

func (p *PasscodeRepository) Delete(ctx context.Context, id string) (int64, error) {
	return DeleteOne(ctx, p.Database, p.tableName, []string{p.idName}, id)
}
