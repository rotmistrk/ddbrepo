package ddbrepo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
	"testing"
	"time"
)

type IdType string
type VersionType int64

type mockTwoKeyStruct struct {
	MyId        IdType    `ddb:"id,hash-key"`
	MyTimestamp time.Time `ddb:"tstamp,range-key"`
	MyValue     string
	Version     VersionType `ddb:",version"`
	ExpireOn    int64       `ddb:"expireOn,expire"`
	Ignored     int32       `ddb:"ignore,ignore"`
	AltHash     string      `ddb:"altKey" ddb-gsi:"alt hash-key"`
	AltRange    string      `ddb-gsi:"alt range-key"`
}

type mockDdbApi struct {
	DynamoDbApi
}

func TestDdbRepo_mangleName(t *testing.T) {
	type fields struct {
		ddbClient               DynamoDbApi
		tableName               string
		waitDuration            time.Duration
		billingMode             types.BillingMode
		keySchema               []types.KeySchemaElement
		attributeDefinitions    []types.AttributeDefinition
		allowUntaggedFields     bool
		lowercaseUntaggedFields bool
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{
			name:   "lowers case if required",
			fields: fields{lowercaseUntaggedFields: true},
			args:   args{"TheName"},
			want:   "theName",
		},
		{
			name:   "preserves case if required",
			fields: fields{lowercaseUntaggedFields: false},
			args:   args{"TheName"},
			want:   "TheName",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &DdbRepo[mockTwoKeyStruct]{
				ddbClient:               tt.fields.ddbClient,
				tableName:               tt.fields.tableName,
				waitDuration:            tt.fields.waitDuration,
				billingMode:             tt.fields.billingMode,
				keySchema:               tt.fields.keySchema,
				attributeDefinitions:    tt.fields.attributeDefinitions,
				allowUntaggedFields:     tt.fields.allowUntaggedFields,
				lowercaseUntaggedFields: tt.fields.lowercaseUntaggedFields,
			}
			if got := repo.mangleName(tt.args.name); got != tt.want {
				t.Errorf("mangleName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDdbRepo_validateConfig(t *testing.T) {
	type fields struct {
		ddbClient               DynamoDbApi
		tableName               string
		waitDuration            time.Duration
		billingMode             types.BillingMode
		keySchema               []types.KeySchemaElement
		attributeDefinitions    []types.AttributeDefinition
		allowUntaggedFields     bool
		lowercaseUntaggedFields bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name:    "invalid when neither conn nor table are set",
			fields:  fields{},
			wantErr: true,
		},
		{
			name:    "invalid when conn not set",
			fields:  fields{tableName: "a-table"},
			wantErr: true,
		},
		{
			name:    "invalid when table name is not set",
			fields:  fields{ddbClient: mockDdbApi{}},
			wantErr: true,
		},
		{
			name:    "valid if table name and ddbapi are set",
			fields:  fields{ddbClient: mockDdbApi{}, tableName: "a-table"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &DdbRepo[mockTwoKeyStruct]{
				ddbClient:               tt.fields.ddbClient,
				tableName:               tt.fields.tableName,
				waitDuration:            tt.fields.waitDuration,
				billingMode:             tt.fields.billingMode,
				keySchema:               tt.fields.keySchema,
				attributeDefinitions:    tt.fields.attributeDefinitions,
				allowUntaggedFields:     tt.fields.allowUntaggedFields,
				lowercaseUntaggedFields: tt.fields.lowercaseUntaggedFields,
			}
			if err := repo.validateConfig(); (err != nil) != tt.wantErr {
				t.Errorf("validateConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNew(t *testing.T) {
	tests := []struct {
		name     string
		wantRepo *DdbRepo[mockTwoKeyStruct]
		wantErr  bool
	}{
		{
			name: "happy path",
			wantRepo: &DdbRepo[mockTwoKeyStruct]{
				waitDuration:             DefaultWaitDuration,
				billingMode:              DefaultBillingMode,
				allowUntaggedFields:      DefaultAllowUntaggedFields,
				lowercaseUntaggedFields:  DefaultLowercaseUntaggedFields,
				readCapacityUnitsConfig:  DefaultReadCapacityUnits,
				writeCapacityUnitsConfig: DefaultWriteCapacityUnits,
				ttlColumn:                "expireOn",
				versionColumn:            "version",
				keySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("id"),
						KeyType:       types.KeyTypeHash,
					},
					{
						AttributeName: aws.String("tstamp"),
						KeyType:       types.KeyTypeRange,
					},
				},
				attributeDefinitions: []types.AttributeDefinition{
					{
						AttributeName: aws.String("id"),
						AttributeType: types.ScalarAttributeTypeS,
					},
					{
						AttributeName: aws.String("tstamp"),
						AttributeType: types.ScalarAttributeTypeN,
					},
					{
						AttributeName: aws.String("altKey"),
						AttributeType: types.ScalarAttributeTypeS,
					},
					{
						AttributeName: aws.String("altRange"),
						AttributeType: types.ScalarAttributeTypeS,
					},
				},
				gsi: map[string]types.GlobalSecondaryIndex{
					"alt": {
						IndexName: aws.String("alt"),
						Projection: &types.Projection{
							ProjectionType: types.ProjectionTypeAll,
						},
						KeySchema: []types.KeySchemaElement{
							{
								AttributeName: aws.String("altKey"),
								KeyType:       types.KeyTypeHash,
							},
							{
								AttributeName: aws.String("altRange"),
								KeyType:       types.KeyTypeRange,
							},
						},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRepo, err := New[mockTwoKeyStruct]()
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRepo == nil {
				t.Errorf("New() returned nil on success, wanted not nil")
			}
			if !reflect.DeepEqual(gotRepo, tt.wantRepo) {
				t.Errorf("New() \n\tgot = %v, \n\twant %v", gotRepo, tt.wantRepo)
			}
		})
	}
}

func Test_attributeDefinition(t *testing.T) {
	type args struct {
		spec       *fieldSpec
		fieldValue reflect.Value
	}
	tests := []struct {
		name    string
		args    args
		want    types.AttributeDefinition
		wantErr bool
	}{
		{
			name: "string ok",
			args: args{
				spec: &fieldSpec{
					name:      "strattr",
					isHashKey: true,
				},
				fieldValue: reflect.ValueOf("hello"),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("strattr"),
				AttributeType: types.ScalarAttributeTypeS,
			},
			wantErr: false,
		},
		{
			name: "int ok",
			args: args{
				spec: &fieldSpec{
					name:       "intattr",
					isRangeKey: true,
				},
				fieldValue: reflect.ValueOf(128),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("intattr"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			wantErr: false,
		},
		{
			name: "time ok",
			args: args{
				spec: &fieldSpec{
					name:       "timeattr",
					isRangeKey: true,
				},
				fieldValue: reflect.ValueOf(time.Now()),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("timeattr"),
				AttributeType: types.ScalarAttributeTypeN,
			},
			wantErr: false,
		},
		{
			name: "bytes ok",
			args: args{
				spec: &fieldSpec{
					name:      "bytes",
					isHashKey: true,
				},
				fieldValue: reflect.ValueOf([]byte("hello")),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("bytes"),
				AttributeType: types.ScalarAttributeTypeB,
			},
			wantErr: false,
		},
		{
			name: "map fails",
			args: args{
				spec: &fieldSpec{
					name:      "map",
					isHashKey: true,
				},
				fieldValue: reflect.ValueOf(make(map[string]string)),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("map"),
			},
			wantErr: true,
		},
		{
			name: "array fails",
			args: args{
				spec: &fieldSpec{
					name:      "array",
					isHashKey: true,
				},
				fieldValue: reflect.ValueOf(make([]string, 0)),
			},
			want: types.AttributeDefinition{
				AttributeName: aws.String("array"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := attributeDefinition(tt.args.spec, tt.args.fieldValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("attributeDefinition() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("attributeDefinition() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_keySchemaElement(t *testing.T) {
	type args struct {
		spec *fieldSpec
	}
	tests := []struct {
		name    string
		args    args
		want    types.KeySchemaElement
		wantErr bool
	}{
		{
			name: "hashKey is",
			args: args{
				&fieldSpec{
					name:      "hk",
					isHashKey: true,
				},
			},
			want: types.KeySchemaElement{
				AttributeName: aws.String("hk"),
				KeyType:       types.KeyTypeHash,
			},
			wantErr: false,
		},
		{
			name: "rangeKey is",
			args: args{
				&fieldSpec{
					name:       "rk",
					isRangeKey: true,
				},
			},
			want: types.KeySchemaElement{
				AttributeName: aws.String("rk"),
				KeyType:       types.KeyTypeRange,
			},
			wantErr: false,
		},
		{
			name: "both-keys-at-once",
			args: args{
				&fieldSpec{
					name:       "bk",
					isHashKey:  true,
					isRangeKey: true,
				},
			},
			want: types.KeySchemaElement{
				AttributeName: aws.String("bk"),
			},
			wantErr: true,
		},
		{
			name: "not-a-key",
			args: args{
				&fieldSpec{
					name: "nk",
				},
			},
			want: types.KeySchemaElement{
				AttributeName: aws.String("nk"),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := keySchemaElement(tt.args.spec)
			if (err != nil) != tt.wantErr {
				t.Errorf("keySchemaElement() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("keySchemaElement() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDdbRepo_HashKeyName(t *testing.T) {
	type fields struct {
		keySchema []types.KeySchemaElement
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr bool
	}{
		{
			name: "success",
			fields: fields{
				keySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("keyRange"),
						KeyType:       types.KeyTypeRange,
					},
					{
						AttributeName: aws.String("keyHash"),
						KeyType:       types.KeyTypeHash,
					},
				},
			},
			want:    "keyHash",
			wantErr: false,
		},
		{
			name: "no hash key",
			fields: fields{
				keySchema: []types.KeySchemaElement{
					{
						AttributeName: aws.String("keyRange"),
						KeyType:       types.KeyTypeRange,
					},
				},
			},
			want:    "",
			wantErr: true,
		},
		{
			name: "no keys",
			fields: fields{
				keySchema: nil,
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &DdbRepo[mockTwoKeyStruct]{
				keySchema: tt.fields.keySchema,
			}
			got, err := repo.HashKeyName()
			if (err != nil) != tt.wantErr {
				t.Errorf("HashKeyName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("HashKeyName() got = %v, want %v", got, tt.want)
			}
		})
	}
}
