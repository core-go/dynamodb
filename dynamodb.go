package dynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"reflect"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type (
	SecondaryIndex struct {
		IndexName string
		Keys      []string
	}
	Config struct {
		Region          string `mapstructure:"region"`
		AccessKeyID     string `mapstructure:"access_key_id"`
		SecretAccessKey string `mapstructure:"secret_access_key"`
	}
)

func Connect(config Config) (*dynamodb.DynamoDB, error) {
	c := &aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
	}
	sess, err := session.NewSession(c)
	if err != nil {
		return nil, err
	}
	db := dynamodb.New(sess)
	return db, nil
}

func BuildQuery(tableName string, index SecondaryIndex, query map[string]interface{}) (*dynamodb.QueryInput, error) {
	if query == nil {
		query := &dynamodb.QueryInput{
			TableName: aws.String(tableName),
			Select:    aws.String(dynamodb.SelectAllAttributes),
		}
		return query, nil
	}
	var keyConditions *expression.KeyConditionBuilder
	var filterConditions *expression.ConditionBuilder
	for _, key := range index.Keys {
		if value, ok := query[key]; ok {
			c := expression.KeyEqual(expression.Key(key), expression.Value(value))
			if keyConditions == nil {
				keyConditions = &c
			} else {
				and := keyConditions.And(c)
				keyConditions = &and
			}
			delete(query, key)
		} else {
			return nil, fmt.Errorf("missing key to query")
		}
	}
	for key, value := range query {
		c := expression.Name(key).Equal(expression.Value(value))
		if filterConditions == nil {
			filterConditions = &c
		} else {
			and := filterConditions.And(c)
			filterConditions = &and
		}
	}
	builder := expression.NewBuilder().WithKeyCondition(*keyConditions)
	if filterConditions != nil {
		builder.WithFilter(*filterConditions)
	}
	if expr, err := builder.Build(); err != nil {
		return nil, err
	} else {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(tableName),
			IndexName:                 aws.String(index.IndexName),
			KeyConditionExpression:    expr.KeyCondition(),
			ExpressionAttributeNames:  expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			FilterExpression:          expr.Filter(),
		}
		return input, nil
	}
}

func Exist(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}) (bool, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return false, err
	}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       keyMap,
	}
	resp, err := db.GetItemWithContext(ctx, input)
	if err != nil {
		return false, err
	}
	if len(resp.Item) == 0 {
		return false, nil
	}
	return true, nil
}

func Find(ctx context.Context, db *dynamodb.DynamoDB, query *dynamodb.QueryInput, modelType reflect.Type) (interface{}, error) {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	result := reflect.New(modelsType).Interface()
	_, err := FindAndDecode(ctx, db, query, result)
	return result, err
}

func FindAndDecode(ctx context.Context, db *dynamodb.DynamoDB, query *dynamodb.QueryInput, result interface{}) (bool, error) {
	output, err := db.QueryWithContext(ctx, query)
	if err != nil {
		return false, err
	}
	if len(output.Items) == 0 {
		return false, nil
	}
	err = dynamodbattribute.UnmarshalListOfMaps(output.Items, result)
	return true, err
}

func FindByIds(ctx context.Context, db *dynamodb.DynamoDB, modelType reflect.Type, tableName string, key string, value []string) (interface{}, []string, error) {
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	result := reflect.New(modelsType).Interface()
	if ok, unProcessedKeys, err := FindByIdsAndDecode(ctx, db, tableName, key, value, result); ok {
		return result, unProcessedKeys, nil
	} else {
		return nil, unProcessedKeys, err
	}
}

func FindByIdsAndDecode(ctx context.Context, db *dynamodb.DynamoDB, tableName string, key string, value []string, result interface{}) (bool, []string, error) {
	var inputKeys []map[string]*dynamodb.AttributeValue
	var unprocessedKeys []string
	for idx := range value {
		k := map[string]*dynamodb.AttributeValue{
			key: {
				S: aws.String(value[idx]),
			},
		}
		inputKeys = append(inputKeys, k)
	}
	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]*dynamodb.KeysAndAttributes{
			tableName: {
				Keys: inputKeys,
			},
		},
	}
	resp, err := db.BatchGetItemWithContext(ctx, input)
	if err != nil {
		return false, unprocessedKeys, err
	}

	for _, v := range resp.UnprocessedKeys {
		unprocessedKeys = append(unprocessedKeys, v.String())
	}
	err = dynamodbattribute.UnmarshalListOfMaps(resp.Responses[tableName], result)
	if err != nil {
		return false, unprocessedKeys, err
	}

	return true, unprocessedKeys, nil
}

func FindOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, modelType reflect.Type, keys []string, id interface{}) (interface{}, error) {
	result := reflect.New(modelType).Interface()
	if ok, err := FindOneAndDecode(ctx, db, tableName, keys, id, result); ok {
		return result, nil
	} else {
		return nil, err
	}
}

func FindOneAndDecode(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}, result interface{}) (bool, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return false, err
	}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       keyMap,
	}
	resp, err := db.GetItemWithContext(ctx, input)
	if err != nil {
		return false, err
	}
	if len(resp.Item) == 0 {
		return false, fmt.Errorf("item not found")
	}
	err = dynamodbattribute.UnmarshalMap(resp.Item, result)
	return true, err
}

func FindOneAndReturnMapData(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}) (bool, map[string]interface{}, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return false, nil, err
	}
	input := &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key:       keyMap,
	}
	resp, err := db.GetItemWithContext(ctx, input)
	if err != nil {
		return false, nil, err
	}
	if len(resp.Item) == 0 {
		return false, nil, fmt.Errorf("item not found")
	}
	result := map[string]interface{}{}
	err = dynamodbattribute.UnmarshalMap(resp.Item, &result)
	return true, result, err
}

func InsertOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	isExit, err := Exist(ctx, db, tableName, keys, ids)
	if err != nil {
		return 0, err
	}
	if isExit {
		return 0, nil
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func InsertOneWithVersion(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}, versionIndex int, versionField string) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	isExit, err := Exist(ctx, db, tableName, keys, ids)
	if err != nil {
		return 0, err
	}
	if isExit {
		return 0, nil
	}

	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	versionType := modelType.Field(versionIndex).Type.String()
	if ok := strings.Contains(versionType, "int"); !ok {
		return 0, fmt.Errorf("not support type's version: %v", versionType)
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	modelMap[versionField] = &dynamodb.AttributeValue{N: aws.String("1")}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpdateOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	expected, err := buildKeyMapWithExpected(keys, ids, true)
	if err != nil {
		return 0, err
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Expected:               expected,
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpdateOneWithVersion(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}, versionIndex int, versionField string) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	oldModel := reflect.New(modelType).Interface()
	itemExist, err := FindOneAndDecode(ctx, db, tableName, keys, ids, oldModel)
	if err != nil {
		return 0, err
	}
	if !itemExist {
		return 0, fmt.Errorf("not found")
	}
	versionType := modelType.Field(versionIndex).Type.String()
	if ok := strings.Contains(versionType, "int"); !ok {
		return 0, fmt.Errorf("not support type's version: %v", versionType)
	}
	currentVersion := reflect.ValueOf(getFieldValueAtIndex(model, versionIndex)).Int()
	oldVersion := reflect.ValueOf(getFieldValueAtIndex(oldModel, versionIndex)).Int()
	if currentVersion != oldVersion {
		return -1, fmt.Errorf("wrong version")
	}
	nextVersion := currentVersion + 1
	expected, err := buildKeyMapWithExpected(keys, ids, true)
	if err != nil {
		return 0, err
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	modelMap[versionField] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(nextVersion, 10))}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Expected:               expected,
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpsertOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, model interface{}) (int64, error) {
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func UpsertOneWithVersion(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model interface{}, versionIndex int, versionField string) (int64, error) {
	ids := getIdValueFromModel(model, keys)
	itemExist, err := Exist(ctx, db, tableName, keys, ids)
	if err != nil {
		if errNotFound := strings.Contains(err.Error(), "not found"); !errNotFound {
			return 0, err
		}
	}
	if itemExist {
		return UpdateOneWithVersion(ctx, db, tableName, keys, model, versionIndex, versionField)
	} else {
		return InsertOneWithVersion(ctx, db, tableName, keys, model, versionIndex, versionField)
	}
}

func DeleteOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, id interface{}) (int64, error) {
	keyMap, err := buildKeyMap(keys, id)
	if err != nil {
		return 0, err
	}
	params := &dynamodb.DeleteItemInput{
		TableName:              aws.String(tableName),
		Key:                    keyMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.DeleteItemWithContext(ctx, params)
	if err != nil {
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func PatchOne(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model map[string]interface{}) (int64, error) {
	idMap := map[string]interface{}{}
	for i := range keys {
		idMap[keys[i]] = model[keys[i]]
		delete(model, keys[i])
	}
	keyMap, err := buildKeyMap(keys, idMap)
	if err != nil {
		return 0, err
	}
	updateBuilder := expression.UpdateBuilder{}
	for key, value := range model {
		updateBuilder = updateBuilder.Set(expression.Name(key), expression.Value(value))
	}
	var cond expression.ConditionBuilder
	for key, value := range idMap {
		if reflect.ValueOf(cond).IsZero() {
			cond = expression.Name(key).Equal(expression.Value(value))
		}
		cond = cond.And(expression.Name(key).Equal(expression.Value(value)))
	}
	expr, _ := expression.NewBuilder().WithUpdate(updateBuilder).WithCondition(cond).Build()
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(tableName),
		Key:                       keyMap,
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.UpdateItemWithContext(ctx, input)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func PatchOneWithVersion(ctx context.Context, db *dynamodb.DynamoDB, tableName string, keys []string, model map[string]interface{}, versionField string) (int64, error) {
	ids := getIdValueFromMap(model, keys)
	if len(ids) == 0 {
		return 0, fmt.Errorf("cannot patch one an Object that do not have ids field")
	}
	itemExist, oldModel, err := FindOneAndReturnMapData(ctx, db, tableName, keys, ids)
	if err != nil {
		return 0, err
	}
	if !itemExist {
		return 0, fmt.Errorf("item not found")

	}
	currentVersion := reflect.ValueOf(model[versionField])
	versionType := currentVersion.Kind().String()
	oldVersion := oldModel[versionField]
	if ok := strings.Contains(versionType, "int"); !ok {
		return 0, fmt.Errorf("not support type's version: %v", versionType)
	}

	if float64(currentVersion.Int()) != oldVersion {
		return -1, fmt.Errorf("wrong version")

	}
	nextVersion := currentVersion.Int() + 1
	expected, err := buildKeyMapWithExpected(keys, ids, true)
	if err != nil {
		return 0, err
	}
	modelMap, err := dynamodbattribute.MarshalMap(model)
	if err != nil {
		return 0, err
	}
	modelMap[versionField] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(nextVersion, 10))}
	params := &dynamodb.PutItemInput{
		TableName:              aws.String(tableName),
		Expected:               expected,
		Item:                   modelMap,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	output, err := db.PutItemWithContext(ctx, params)
	if err != nil {
		if strings.Index(err.Error(), "ConditionalCheckFailedException:") >= 0 {
			return 0, fmt.Errorf("object not found")
		}
		return 0, err
	}
	return int64(aws.Float64Value(output.ConsumedCapacity.CapacityUnits)), nil
}

func GetFieldByName(modelType reflect.Type, fieldName string) (int, string, bool) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if field.Name == fieldName {
			if dynamodbTag, ok := field.Tag.Lookup("dynamodbav"); ok {
				name := strings.Split(dynamodbTag, ",")[0]
				return index, name, true
			}
			if jsonTag, ok := field.Tag.Lookup("json"); ok {
				name := strings.Split(jsonTag, ",")[0]
				return index, name, true
			}
		}
	}
	return -1, fieldName, false
}

func GetFieldByIndex(modelType reflect.Type, fieldIndex int) (fieldName, tagName string, isExit bool) {
	if fieldIndex < modelType.NumField() {
		field := modelType.Field(fieldIndex)
		if dynamodbTag, ok := field.Tag.Lookup("dynamodbav"); ok {
			name := strings.Split(dynamodbTag, ",")[0]
			return field.Name, name, true
		}
		if jsonTag, ok := field.Tag.Lookup("json"); ok {
			name := strings.Split(jsonTag, ",")[0]
			return field.Name, name, true
		}
		return field.Name, field.Name, true
	}
	return "", "", false
}

func GetFieldByTagName(modelType reflect.Type, tagName string) (int, string, bool) {
	numField := modelType.NumField()
	for index := 0; index < numField; index++ {
		field := modelType.Field(index)
		if dbTag, ok := field.Tag.Lookup("dynamodbav"); ok && strings.Split(dbTag, ",")[0] == tagName {
			return index, field.Name, true
		}
		if jsonTag, ok := field.Tag.Lookup("json"); ok && strings.Split(jsonTag, ",")[0] == tagName {
			return index, field.Name, true
		}
	}
	return -1, tagName, false
}

func getIdValueFromModel(model interface{}, keys []string) []interface{} {
	var values []interface{}
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	for idx := range keys {
		if index, _, ok := GetFieldByTagName(modelValue.Type(), keys[idx]); ok {
			idValue := modelValue.Field(index).Interface()
			values = append(values, idValue)
		}
	}
	return values
}

func getFieldValueAtIndex(model interface{}, index int) interface{} {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	return modelValue.Field(index).Interface()
}

func setValueWithIndex(model interface{}, index int, value interface{}) (interface{}, error) {
	valueObject := reflect.Indirect(reflect.ValueOf(model))
	numField := valueObject.NumField()
	if index >= 0 && index < numField {
		valueObject.Field(index).Set(reflect.ValueOf(value))
		return model, nil
	}
	return nil, fmt.Errorf("error no found field index: %v", index)
}

func buildKeyMap(keys []string, value interface{}) (map[string]*dynamodb.AttributeValue, error) {
	idValue := reflect.ValueOf(value)
	idMap := map[string]interface{}{}
	switch idValue.Kind() {
	case reflect.Map:
		for _, key := range keys {
			if !idValue.MapIndex(reflect.ValueOf(key)).IsValid() {
				return nil, fmt.Errorf("wrong mapping key and value")
			}
			idMap[key] = idValue.MapIndex(reflect.ValueOf(key)).Interface()
		}
		if len(idMap) != idValue.Len() {
			return nil, fmt.Errorf("wrong mapping key and value")
		}
	case reflect.Slice, reflect.Array:
		if len(keys) != idValue.Len() {
			return nil, fmt.Errorf("wrong mapping key and value")
		}
		for idx := range keys {
			idMap[keys[idx]] = idValue.Index(idx).Interface()
		}
	default:
		idMap[keys[0]] = idValue.Interface()
	}
	keyMap := map[string]*dynamodb.AttributeValue{}
	for key, value := range idMap {
		v := reflect.ValueOf(value)
		switch v.Kind() {
		case reflect.String:
			keyMap[key] = &dynamodb.AttributeValue{S: aws.String(v.String())}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			keyMap[key] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatInt(v.Int(), 10))}
		case reflect.Float32, reflect.Float64:
			keyMap[key] = &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%g", v.Float()))}
		default:
			return keyMap, fmt.Errorf("data type not support")
		}
	}
	return keyMap, nil
}

func buildKeyMapWithExpected(keys []string, model interface{}, isExist bool) (map[string]*dynamodb.ExpectedAttributeValue, error) {
	values := getIdValueFromModel(model, keys)
	if len(values) == 0 {
		return nil, fmt.Errorf("cannot update one an Object that do not have ids field")
	}
	keyMap, err := buildKeyMap(keys, values)
	if err != nil {
		return nil, err
	}
	result := map[string]*dynamodb.ExpectedAttributeValue{}
	for k, v := range keyMap {
		result[k] = &dynamodb.ExpectedAttributeValue{
			Value:  v,
			Exists: aws.Bool(isExist),
		}
	}
	return result, nil
}

func updateModelVersion(model interface{}, versionIndex int) {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := getFieldValueAtIndex(model, versionIndex)

	switch reflect.ValueOf(currentVersion).Kind() {
	case reflect.Int:
		nextVersion := reflect.ValueOf(currentVersion.(int) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	case reflect.Int32:
		nextVersion := reflect.ValueOf(currentVersion.(int32) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	case reflect.Int64:
		nextVersion := reflect.ValueOf(currentVersion.(int64) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	default:
		panic("version's type not supported")
	}
}

func FindTableDescription(db *dynamodb.DynamoDB, tableName string) (*dynamodb.TableDescription, error) {
	req := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	result, err := db.DescribeTable(req)
	if err != nil {
		return nil, err
	}
	return result.Table, nil
}

func MapToDBObject(object map[string]interface{}, objectMap map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range object {
		field := objectMap[key]
		result[field] = value
	}
	return result
}

func MakeMapObject(modelType reflect.Type) map[string]string {
	maps := make(map[string]string)
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		key := modelType.Field(i).Name
		field, _ := modelType.FieldByName(key)
		if jsonTag, ok := field.Tag.Lookup("json"); ok {
			tag := strings.Split(jsonTag, ",")[0]
			if dbTag, ok := field.Tag.Lookup("dynamodbav"); ok {
				maps[tag] = strings.Split(dbTag, ",")[0]
			} else {
				maps[tag] = tag
			}
		} else {
			maps[key] = key
		}
	}
	return maps
}

func updateMapVersion(data map[string]interface{}, versionFieldName string) {
	if currentVersion, exist := data[versionFieldName]; exist {
		switch reflect.ValueOf(currentVersion).Kind() {
		case reflect.Int:
			data[versionFieldName] = currentVersion.(int) + 1
		case reflect.Int32:
			data[versionFieldName] = currentVersion.(int32) + 1
		case reflect.Int64:
			data[versionFieldName] = currentVersion.(int64) + 1
		default:
			panic("version's type not supported")
		}
	}
}

func getIdValueFromMap(model map[string]interface{}, keys []string) []interface{} {
	var values []interface{}
	for _, key := range keys {
		if id, exist := model[key]; exist {
			values = append(values, id)
		}
	}
	return values
}
