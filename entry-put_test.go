package ddbrepo

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rotmistrk/must"
	"reflect"
	"testing"
	"time"
)

func TestAttributeExists(t *testing.T) {
	type args struct {
		attrName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "positive",
			args: args{"theKey"},
			want: "attribute_exists(theKey)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AttributeExists(tt.args.attrName); got != tt.want {
				t.Errorf("AttributeExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAttributeNotExists(t *testing.T) {
	type args struct {
		attrName string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "positive",
			args: args{"theKey"},
			want: "attribute_not_exists(theKey)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := AttributeNotExists(tt.args.attrName); got != tt.want {
				t.Errorf("AttributeNotExists() = %v, want %v", got, tt.want)
			}
		})
	}
}

type PartType int

type sampleRecord struct {
	ID       string   `ddb:",hash-key"`
	Part     PartType `ddb:",range-key"`
	Version  int      `ddb:"version,version"`
	Name     string   `ddb:"name"`
	ExpireOn int64    `ddb:",expire"`
}

func TestOpForPut(t *testing.T) {
	type args struct {
		repo  PutWorkflowColumns
		entry *sampleRecord
		op    func(repo PutWorkflowColumns, entry map[string]types.AttributeValue) (string, map[string]types.AttributeValue, error)
	}
	repo, err := New[sampleRecord]()
	if err != nil {
		t.Fatal(err)
	}
	if repo.versionColumn != "version" {
		t.Errorf("version column name is wrong: got %v", repo.versionColumn)
	}
	if exp, ok := repo.ExpirationFieldName(); !ok || exp != "expireOn" {
		t.Errorf("expiration field name is wrong: got %v (present=%v)", exp, ok)
	}
	tests := []struct {
		name    string
		args    args
		want    string
		param   map[string]types.AttributeValue
		wantErr bool
	}{
		{
			name: "replace",
			args: args{
				repo:  repo,
				entry: &sampleRecord{},
				op:    Replace,
			},
			want:    "",
			param:   nil,
			wantErr: false,
		},
		{
			name: "update",
			args: args{
				repo:  must.Must(New[sampleRecord]()),
				entry: &sampleRecord{},
				op:    Update,
			},
			want:    "attribute_exists(ID)",
			param:   nil,
			wantErr: false,
		},
		{
			name: "insert",
			args: args{
				repo:  must.Must(New[sampleRecord]()),
				entry: &sampleRecord{},
				op:    Insert,
			},
			want:    "attribute_not_exists(ID)",
			param:   nil,
			wantErr: false,
		},
		{
			name: "insert or replace expired",
			args: args{
				repo: must.Must(New[sampleRecord]()),
				entry: &sampleRecord{
					ID:       "one",
					Part:     11,
					Version:  1,
					Name:     "une",
					ExpireOn: 55555,
				},
				op: InsertOrReplaceExpired,
			},
			want: "attribute_not_exists(ID) or (expireOn < :expireOn)",
			param: map[string]types.AttributeValue{
				":expireOn": &types.AttributeValueMemberN{
					// race condition is possible
					Value: fmt.Sprint(time.Now().Unix()),
				},
			},
			wantErr: false,
		},
		{
			name: "is next version",
			args: args{
				repo: must.Must(New[sampleRecord]()),
				entry: &sampleRecord{
					ID:       "one",
					Version:  2,
					Name:     "une",
					ExpireOn: 55555,
				},
				op: IsNextVersion,
			},
			want: "version = :version",
			param: map[string]types.AttributeValue{
				":version": &types.AttributeValueMemberN{
					Value: "1",
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.args.op(tt.args.repo, must.Must(Marshal(repo, tt.args.entry)))
			if (err != nil) != tt.wantErr {
				t.Errorf("%v error = %v, wantErr %v", tt.name, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("%v got = %v, want %v", tt.name, got, tt.want)
			}
			if len(got1) != len(tt.param) {
				t.Errorf("%v got1 = %v items, want %v items", tt.name, len(got1), len(tt.param))
			}
			for k, v := range tt.param {
				if !reflect.DeepEqual(v, got1[k]) {
					t.Errorf("Insert() got1 = %v, want %v", got1[k], v)
				}
			}
		})
	}
}
