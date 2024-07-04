package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type QueryOption func(query *dynamodb.QueryInput)

func QueryHkCbk[R any](repo *DdbRepo[R], callback func(r *R) error, source R, condition ...QueryOption) error {
	hashKeyName, err := repo.HashKeyName()
	if err != nil {
		return err
	}
	key, err := MarshalTagFilter(repo, source, IncludeHashKey, ":")
	if err != nil {
		return err
	}
	input := &dynamodb.QueryInput{
		TableName:                 aws.String(repo.tableName),
		KeyConditionExpression:    aws.String(hashKeyName + "= :" + hashKeyName),
		ExpressionAttributeValues: key,
		ExclusiveStartKey:         nil,
	}
	for _, c := range condition {
		c(input)
	}
	for {
		if output, err := repo.ddbClient.Query(context.TODO(), input); err != nil {
			return err
		} else {
			for _, item := range output.Items {
				var record R
				if err := Unmarshal(repo, &record, item); err != nil {
					return err
				} else if err = callback(&record); err != nil {
					return err
				}
			}
			if output.LastEvaluatedKey == nil {
				break
			} else {
				input.ExclusiveStartKey = output.LastEvaluatedKey
			}
		}
	}

	return nil
}
