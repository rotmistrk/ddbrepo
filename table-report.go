package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableReportSummary struct {
	Table *types.TableDescription
	Ttl   *types.TimeToLiveDescription
}

func (repo *DdbRepo[T]) TableReport() (*TableReportSummary, error) {
	if err := repo.validateConfig(); err != nil {
		return nil, err
	}
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(repo.tableName),
	}

	if output, err := repo.ddbClient.DescribeTable(context.TODO(), input); err != nil {
		return nil, err
	} else {
		ttlInput := &dynamodb.DescribeTimeToLiveInput{
			TableName: aws.String(repo.tableName),
		}
		if ttlOutput, err := repo.ddbClient.DescribeTimeToLive(context.TODO(), ttlInput); err != nil {
			return nil, err
		} else {
			return &TableReportSummary{
				Table: output.Table,
				Ttl:   ttlOutput.TimeToLiveDescription,
			}, err
		}
	}
}
