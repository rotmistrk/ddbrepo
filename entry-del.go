package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func (repo DdbRepo[RecordType]) DelItemOp(record *RecordType) error {
	if key, err := MarshalKey(&repo, record, ""); err != nil {
		return err
	} else {
		input := &dynamodb.DeleteItemInput{
			TableName: aws.String(repo.tableName),
			Key:       key,
		}
		_, err := repo.ddbClient.DeleteItem(context.TODO(), input)
		return err
	}
}
