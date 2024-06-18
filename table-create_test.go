package ddbrepo

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"testing"
)

type expectingMockedCreate struct {
	DynamoDbApi
	t *testing.T
}

func (api *expectingMockedCreate) CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	expected := &dynamodb.CreateTableInput{
		TableName:   aws.String("my-table"),
		BillingMode: DefaultBillingMode,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("tstamp"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("altKey"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("altRange"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("tstamp"), KeyType: types.KeyTypeRange},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(DefaultReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(DefaultWriteCapacityUnits),
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("alt"),
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("altKey"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("altRange"), KeyType: types.KeyTypeRange},
				},
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},
			},
		},
	}
	if !reflect.DeepEqual(params, expected) {
		msg := fmt.Sprintf("Unexpected request:\n* got\t%v\n* want\t%v", JsonLine(params), JsonLine(expected))
		api.t.Error(msg)
		return nil, errors.New(msg)
	}
	return &dynamodb.CreateTableOutput{}, nil
}

func (api *expectingMockedCreate) UpdateTimeToLive(ctx context.Context, params *dynamodb.UpdateTimeToLiveInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	expected := &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String("my-table"),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("expireOn"),
			Enabled:       aws.Bool(true),
		},
	}
	if !reflect.DeepEqual(params, expected) {
		msg := fmt.Sprintf("Unexpected request:\n* got\t%v\n* want\t%v", JsonLine(params), JsonLine(expected))
		api.t.Error(msg)
		return nil, errors.New(msg)
	}
	return &dynamodb.UpdateTimeToLiveOutput{}, nil
}

func (api *expectingMockedCreate) DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	expected := &dynamodb.DescribeTableInput{
		TableName: aws.String("my-table"),
	}
	if !reflect.DeepEqual(expected, params) {
		msg := fmt.Sprintf("Unexpected request:\n* got\t%v\n* want\t%v", JsonLine(params), JsonLine(expected))
		api.t.Error(msg)
		return nil, errors.New(msg)
	}
	return &dynamodb.DescribeTableOutput{
		Table: &types.TableDescription{
			TableName:   aws.String("my-table"),
			TableStatus: types.TableStatusActive,
		},
	}, nil
}

func TestDdbRepo_TableCreate(t *testing.T) {
	repo, err := New[mockTwoKeyStruct]()
	if err != nil {
		t.Errorf("Unexpected failure to create repo")
	}
	dbapi := &expectingMockedCreate{t: t}
	tests := []struct {
		name    string
		fields  *DdbRepo[mockTwoKeyStruct]
		wantErr bool
	}{
		{
			name:    "happy path",
			fields:  repo.WithTableName("my-table").WithDynamoDbApi(dbapi),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fields.TableCreate(); (err != nil) != tt.wantErr {
				t.Errorf("TableCreate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
