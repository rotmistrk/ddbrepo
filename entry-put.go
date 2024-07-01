package ddbrepo

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"strconv"
	"time"
)

type PutWorkflowColumns interface {
	HashKeyName() (string, error)
	VersionFieldName() (string, error)
	ExpirationFieldName() (string, bool)
}

type PutItemOp func(
	repo PutWorkflowColumns,
	entry map[string]types.AttributeValue,
) (
	cond string,
	param map[string]types.AttributeValue,
	err error,
)

func (repo DdbRepo[RecordType]) PutItem(entry *RecordType) error {
	return repo.PutItemOp(entry, Replace)
}

func Replace(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error) {
	return "", nil, nil
}

func Insert(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error) {
	if keyName, err := repo.HashKeyName(); err != nil {
		return "", nil, err
	} else {
		return AttributeNotExists(keyName), nil, nil
	}
}

func InsertOrReplaceExpired(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error) {
	if keyName, err := repo.HashKeyName(); err != nil {
		return "", nil, err
	} else {
		cond := AttributeNotExists(keyName)
		values := make(map[string]types.AttributeValue)
		if expname, ok := repo.ExpirationFieldName(); ok {
			cond += fmt.Sprintf(" or (%v < :%v)", expname, expname)
			values[":"+expname] = &types.AttributeValueMemberN{
				Value: fmt.Sprintf("%v", time.Now().Unix()),
			}
		}
		return cond, values, nil
	}
}

func Update(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error) {
	if keyName, err := repo.HashKeyName(); err != nil {
		return "", nil, err
	} else {
		return AttributeExists(keyName), nil, nil
	}
}

func IsNextVersion(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error) {
	if vers, err := repo.VersionFieldName(); err != nil {
		return "", nil, err
	} else if vers == "" {
		return "", nil, errors.New("no version column defined")
	} else if value, found := entry[vers]; found {
		value, err = incrementNumericValueBy(value, -1)
		if err != nil {
			return "", nil, err
		}
		param := map[string]types.AttributeValue{
			":" + vers: value,
		}
		cond := fmt.Sprintf("%v = :%v", vers, vers)
		return cond, param, nil
	} else {
		return "", nil, fmt.Errorf("failed to find column %v value as next version", vers)
	}
}

func incrementNumericValueBy(value types.AttributeValue, increment int) (types.AttributeValue, error) {
	switch v := value.(type) {
	case *types.AttributeValueMemberN:
		if v1, err := strconv.Atoi(v.Value); err != nil {
			return nil, err
		} else {
			value = &types.AttributeValueMemberN{
				Value: fmt.Sprint(v1 + increment),
			}
		}
	default:
		return nil, fmt.Errorf("invalid version type %T", value)
	}
	return value, nil
}

func (repo DdbRepo[RecordType]) PutItemOp(entry *RecordType, op PutItemOp) error {
	item, err := Marshal(&repo, entry)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(repo.tableName),
		Item:      item,
	}
	condStr, param, err := op(&repo, item)
	if err != nil {
		return err
	}
	if condStr != "" {
		input.ConditionExpression = aws.String(condStr)
	}
	input.ExpressionAttributeValues = param
	_, err = repo.ddbClient.PutItem(context.TODO(), input)
	return err
}

func AttributeExists(attrName string) string {
	return fmt.Sprintf("attribute_exists(%v)", attrName)
}

func AttributeNotExists(attrName string) string {
	return fmt.Sprintf("attribute_not_exists(%v)", attrName)
}

func (repo DdbRepo[RecordType]) PutConditional(entry *RecordType, condition string, conditionValues map[string]types.AttributeValue) error {
	item, err := Marshal(&repo, entry)
	if err != nil {
		return err
	}
	input := &dynamodb.PutItemInput{
		TableName:                 aws.String(repo.tableName),
		Item:                      item,
		ConditionExpression:       aws.String(condition),
		ExpressionAttributeValues: conditionValues,
	}
	_, err = repo.ddbClient.PutItem(context.TODO(), input)
	return err
}
