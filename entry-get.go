package ddbrepo

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (repo DdbRepo[RecordType]) GetItem(record *RecordType) error {
	if record == nil {
		return errors.New("record pointer is required")
	}
	if err := repo.validateConfig(); err != nil {
		return err
	}
	if key, err := MarshalKey(&repo, record, ""); err != nil {
		return err
	} else {
		input := &dynamodb.GetItemInput{
			TableName: aws.String(repo.tableName),
			Key:       key,
		}
		if output, err := repo.ddbClient.GetItem(context.TODO(), input); err != nil {
			return err
		} else {
			if output.Item == nil {
				return errors.New("item not found in " + repo.tableName)
			} else {
				return Unmarshal(&repo, record, output.Item)
			}
		}
	}
}
