package ddbrepo

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rotmistrk/must"
	"reflect"
	"testing"
	"time"
)

func TestIncludeAll(t *testing.T) {
	type args struct {
		spec fieldSpec
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"success",
			args{fieldSpec{}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IncludeAll(tt.args.spec); got != tt.want {
				t.Errorf("IncludeAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

type KeyType string

type samplteStruct struct {
	Key    KeyType `ddb:"hkey, hash-key"`
	Range  string  `ddb:"rkey, range-key"`
	Value  string
	Expire time.Time `ddb:"expire, expire"`
}

func getAv(val interface{}) types.AttributeValue {
	result, err := attributevalue.Marshal(val)
	if err != nil {
		panic(err)
	}
	return result
}

func TestMarshalTagFilter(t *testing.T) {
	type args struct {
		props  parseProps
		source interface{}
		filter func(spec fieldSpec) bool
	}
	when := must.Must(time.Parse(time.RFC3339, "2022-01-01T00:00:00Z"))
	tests := []struct {
		name       string
		args       args
		wantResult map[string]types.AttributeValue
		wantErr    bool
	}{
		{
			name: "basic case",
			args: args{
				props: &props{true, true},
				source: &samplteStruct{
					Key:    "key",
					Range:  "range",
					Value:  "value",
					Expire: when,
				},
				filter: IncludeAll,
			},
			wantResult: map[string]types.AttributeValue{
				"hkey":   getAv("key"),
				"rkey":   getAv("range"),
				"value":  getAv("value"),
				"expire": getAv(when),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotResult, err := MarshalTagFilter(tt.args.props, tt.args.source, tt.args.filter, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalTagFilter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("MarshalTagFilter() gotResult = %v, want %v", gotResult, tt.wantResult)
			}
			fmt.Println(JsonPretty(&gotResult, "| ", "  "))
		})
	}
}
