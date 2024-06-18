package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (repo *DdbRepo[T]) TableDelete() (err error) {
	if err = repo.validateConfig(); err != nil {
		return
	}
	input := &dynamodb.DeleteTableInput{
		TableName: aws.String(repo.tableName),
	}

	if _, err := repo.ddbClient.DeleteTable(context.TODO(), input); err != nil {
		return err
	}

	return
}
