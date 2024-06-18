package ddbrepo

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"strings"
	"time"
)

const (
	DefaultWaitDuration            = 5 * time.Minute
	DefaultAllowUntaggedFields     = true
	DefaultLowercaseUntaggedFields = true
	DefaultBillingMode             = types.BillingModeProvisioned
	DefaultReadCapacityUnits       = 1
	DefaultWriteCapacityUnits      = 1
)

func Must[arg any](action func() (arg, error)) arg {
	if result, err := action(); err == nil {
		return result
	} else {
		panic(err)
	}
}

type DdbRepo[RecordType any] struct {
	ddbClient                DynamoDbApi
	tableName                string
	ttlColumn                string
	versionColumn            string
	waitDuration             time.Duration
	billingMode              types.BillingMode
	keySchema                []types.KeySchemaElement
	attributeDefinitions     []types.AttributeDefinition
	allowUntaggedFields      bool
	lowercaseUntaggedFields  bool
	readCapacityUnitsConfig  int64
	writeCapacityUnitsConfig int64
	gsi                      map[string]types.GlobalSecondaryIndex
}

func (repo *DdbRepo[T]) VersionFieldName() (string, error) {
	return repo.versionColumn, nil
}

func (repo *DdbRepo[T]) HashKeyName() (string, error) {
	if repo.keySchema != nil {
		for _, key := range repo.keySchema {
			if key.KeyType == types.KeyTypeHash {
				return *key.AttributeName, nil
			}
		}
	}
	return "", fmt.Errorf("no key attribute found")
}

func New[T any]() (repo *DdbRepo[T], err error) {
	defer func() {
		if r := recover(); r != nil {
			repo = nil
			err = errors.New(fmt.Sprintf("impossible to (un)marshal: %v", r))
		}
	}()

	repo = &DdbRepo[T]{
		waitDuration:             DefaultWaitDuration,
		billingMode:              DefaultBillingMode,
		allowUntaggedFields:      DefaultAllowUntaggedFields,
		lowercaseUntaggedFields:  DefaultLowercaseUntaggedFields,
		attributeDefinitions:     make([]types.AttributeDefinition, 0, 2),
		keySchema:                make([]types.KeySchemaElement, 0, 2),
		readCapacityUnitsConfig:  DefaultReadCapacityUnits,
		writeCapacityUnitsConfig: DefaultWriteCapacityUnits,
	}

	var sample T
	value := reflect.ValueOf(&sample).Elem()
	target := reflect.TypeOf(&sample).Elem()
	if value.Kind() != reflect.Struct {
		err = errors.New(fmt.Sprintf("ddb repo can't store %v as a record", sample))
	}
	hashKeys, rangeKeys, keys := 0, 0, 0
	for i, I := 0, target.NumField(); i < I; i++ {
		fieldType := target.Field(i)
		fieldValue := value.Field(i)
		if spec, err := newFieldSpec(repo, &fieldType); err != nil {
			return nil, err
		} else {
			if spec.IsKey() {
				if def, err := attributeDefinition(spec, fieldValue); err != nil {
					return nil, err
				} else {
					repo.attributeDefinitions = append(repo.attributeDefinitions, def)
				}
				if key, err := keySchemaElement(spec); err != nil {
					return nil, err
				} else {
					repo.keySchema = append(repo.keySchema, key)
				}
				if spec.IsHashKey() {
					hashKeys++
				}
				if spec.IsRangeKey() {
					rangeKeys++
				}
				keys++
			}
			if spec.IsTtlField() {
				repo.ttlColumn = repo.mangleName(spec.name)
			}
			if spec.IsVersionField() {
				repo.versionColumn = repo.mangleName(spec.name)
			}
			if spec.gsiHash != nil {
				if repo.gsi == nil {
					repo.gsi = make(map[string]types.GlobalSecondaryIndex)
				}
				for k, v := range spec.gsiHash {
					gsi := repo.gsi[k]
					if gsi.IndexName == nil {
						gsi.IndexName = aws.String(k)
						gsi.KeySchema = make([]types.KeySchemaElement, 0)
						gsi.Projection = &types.Projection{ProjectionType: types.ProjectionTypeAll}
					}
					keyType := types.KeyTypeHash
					if !v {
						keyType = types.KeyTypeRange
					}
					gsi.KeySchema = append(gsi.KeySchema, types.KeySchemaElement{
						AttributeName: aws.String(spec.name),
						KeyType:       keyType,
					})
					hasAd := false
					for _, ad := range repo.attributeDefinitions {
						if *ad.AttributeName == spec.name {
							hasAd = true
							break
						}
					}
					if !hasAd {
						if def, err := attributeDefinition(spec, fieldValue); err != nil {
							return nil, err
						} else {
							repo.attributeDefinitions = append(repo.attributeDefinitions, def)
						}
					}
					repo.gsi[k] = gsi
				}
			}
		}
	}
	if keys > 2 || keys != hashKeys+rangeKeys || hashKeys != 1 || rangeKeys > 1 {
		return repo, errors.New(fmt.Sprintf("invalid keys configuration: %v hash, %v range, %v total", hashKeys, rangeKeys, keys))
	}
	return repo, nil
}

func (repo *DdbRepo[RecordType]) TableName() string {
	return repo.tableName
}

func keySchemaElement(spec *fieldSpec) (types.KeySchemaElement, error) {
	result := types.KeySchemaElement{
		AttributeName: aws.String(spec.name),
	}
	if spec.IsHashKey() && !spec.IsRangeKey() {
		result.KeyType = types.KeyTypeHash
	} else if spec.IsRangeKey() && !spec.IsHashKey() {
		result.KeyType = types.KeyTypeRange
	} else {
		return result, errors.New(fmt.Sprintf("ambiguous key specifications for %v", spec.name))
	}
	return result, nil
}

func attributeDefinition(spec *fieldSpec, fieldValue reflect.Value) (types.AttributeDefinition, error) {
	ret := types.AttributeDefinition{
		AttributeName: aws.String(spec.name),
	}
	switch fieldValue.Interface().(type) {
	case string:
		ret.AttributeType = types.ScalarAttributeTypeS
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, time.Time:
		ret.AttributeType = types.ScalarAttributeTypeN
	case []byte:
		ret.AttributeType = types.ScalarAttributeTypeB
	default:
		return ret, errors.New(fmt.Sprintf("type %t is not supported as key for field %v", fieldValue.Interface(), spec.name))
	}
	return ret, nil
}

func (repo *DdbRepo[T]) SetTableName(tagleName string) {
	repo.tableName = tagleName
}

func (repo *DdbRepo[T]) SetAwsConfig(cfg aws.Config) {
	repo.ddbClient = dynamodb.NewFromConfig(cfg)
}

func (repo DdbRepo[T]) WithTableName(tableName string) *DdbRepo[T] {
	repo.tableName = tableName
	return &repo
}

func (repo DdbRepo[T]) WithDynamoDbApi(api DynamoDbApi) *DdbRepo[T] {
	repo.ddbClient = api
	return &repo
}

func (repo DdbRepo[T]) WithAwsConfig(cfg aws.Config) DdbRepo[T] {
	repo.ddbClient = dynamodb.NewFromConfig(cfg)
	return repo
}

func (repo *DdbRepo[T]) validateConfig() error {
	if repo.tableName == "" {
		return errors.New("table name is required")
	}
	if repo.ddbClient == nil {
		return errors.New("ddb connection is required")
	}
	return nil
}

func (repo *DdbRepo[T]) AllowUntaggedFields() bool {
	return repo.allowUntaggedFields
}

func (repo *DdbRepo[T]) mangleName(name string) string {
	if repo.LowercaseUntaggedFields() && len(name) > 0 {
		return strings.ToLower(name[0:1]) + name[1:]
	} else {
		return name
	}
}

func (repo *DdbRepo[T]) LowercaseUntaggedFields() bool {
	return repo.lowercaseUntaggedFields
}

func (repo *DdbRepo[T]) getProvisionedThroughput() *types.ProvisionedThroughput {
	return &types.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(repo.readCapacityUnitsConfig),
		WriteCapacityUnits: aws.Int64(repo.writeCapacityUnitsConfig),
	}
}
