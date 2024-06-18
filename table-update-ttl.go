package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (repo *DdbRepo[T]) TableUpdateTtl() error {
	input := &dynamodb.UpdateTimeToLiveInput{
		TableName:               aws.String(repo.tableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{},
	}
	if repo.ttlColumn != "" {
		input.TimeToLiveSpecification.AttributeName = aws.String(repo.ttlColumn)
		input.TimeToLiveSpecification.Enabled = aws.Bool(true)
	} else {
		input.TimeToLiveSpecification.Enabled = aws.Bool(false)
	}
	if _, err := repo.ddbClient.UpdateTimeToLive(context.TODO(), input); err != nil {
		return err
	} else {
		return nil
	}
}
