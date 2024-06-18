package ddbrepo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

func (repo *DdbRepo[T]) TableCreate() (err error) {
	if err = repo.validateConfig(); err != nil {
		return
	}
	input := &dynamodb.CreateTableInput{
		TableName:              aws.String(repo.tableName),
		AttributeDefinitions:   repo.getAttributeDefinitions(),
		KeySchema:              repo.getKeySchema(),
		GlobalSecondaryIndexes: repo.getGlobalSecondaryIndexes(),
		LocalSecondaryIndexes:  repo.getLocalSecondaryIndexes(),
		BillingMode:            repo.getBillingMode(),
		ProvisionedThroughput:  repo.getProvisionedThroughput(),
	}

	if repo.gsi != nil {
		input.GlobalSecondaryIndexes = make([]types.GlobalSecondaryIndex, 0, len(repo.gsi))
		for _, v := range repo.gsi {
			v.ProvisionedThroughput = &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(repo.readCapacityUnitsConfig),
				WriteCapacityUnits: aws.Int64(repo.writeCapacityUnitsConfig),
			}
			input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, v)
		}
	}

	if _, err := repo.ddbClient.CreateTable(context.TODO(), input); err != nil {
		return err
	}

	if errr := repo.WaitTillReady(); errr != nil {
		return errr
	}

	if repo.ttlColumn != "" {
		return repo.TableUpdateTtl()
	}

	return
}

func (repo DdbRepo[RecordType]) WaitTillReady() error {
	if err := repo.validateConfig(); err != nil {
		return err
	}
	waiter := dynamodb.NewTableExistsWaiter(repo.ddbClient)
	return waiter.Wait(context.TODO(), repo.getDescribeTableInput(), repo.getWaitDuration())
}

func (repo DdbRepo[RecordType]) getAttributeDefinitions() []types.AttributeDefinition {
	return repo.attributeDefinitions
}

func (repo *DdbRepo[T]) getKeySchema() []types.KeySchemaElement {
	return repo.keySchema
}

func (repo *DdbRepo[T]) getGlobalSecondaryIndexes() []types.GlobalSecondaryIndex {
	return nil
}

func (repo *DdbRepo[T]) getLocalSecondaryIndexes() []types.LocalSecondaryIndex {
	return nil
}

func (repo *DdbRepo[T]) getBillingMode() types.BillingMode {
	return repo.billingMode
}

func (repo *DdbRepo[T]) getDescribeTableInput() *dynamodb.DescribeTableInput {
	return &dynamodb.DescribeTableInput{
		TableName: aws.String(repo.tableName),
	}
}

func (repo *DdbRepo[T]) getWaitDuration() time.Duration {
	return repo.waitDuration
}
