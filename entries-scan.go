package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ScanOption func(input *dynamodb.ScanInput) error

func ScanIndex(indexName string) ScanOption {
	return func(input *dynamodb.ScanInput) error {
		input.IndexName = aws.String(indexName)
		return nil
	}
}

func ScanCondition(expression string, param map[string]types.AttributeValue) ScanOption {
	return func(input *dynamodb.ScanInput) error {
		input.FilterExpression = aws.String(expression)
		input.ExpressionAttributeValues = param
		return nil
	}
}

func (repo *DdbRepo[RecordType]) ScanCbk(callback func(record *RecordType) error, options ...ScanOption) error {
	input := &dynamodb.ScanInput{
		TableName: aws.String(repo.tableName),
	}
	for _, option := range options {
		if err := option(input); err != nil {
			return err
		}
	}
	for {
		output, err := repo.ddbClient.Scan(context.TODO(), input)
		if err != nil {
			return err
		}
		for _, item := range output.Items {
			var result RecordType
			if err = Unmarshal(repo, &result, item); err != nil {
				return err
			}
			if err = callback(&result); err != nil {
				return err
			}
		}
		if output.LastEvaluatedKey == nil {
			return nil
		}
		input.ExpressionAttributeValues = output.LastEvaluatedKey
	}
}
