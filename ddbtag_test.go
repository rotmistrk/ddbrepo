package ddbrepo

import (
	"reflect"
	"testing"
	"time"
)

type props struct {
	allowUntagged     bool
	lowercaseUntagged bool
}

func (p *props) AllowUntaggedFields() bool {
	return p.allowUntagged
}

func (p *props) LowercaseUntaggedFields() bool {
	return p.lowercaseUntagged
}

type testStruct struct {
	privateField       string
	privateTaggedField string `ddb:"required"`
	PublicField        string
	PublicIgnoredField string    `ddb:",ignore"`
	HashKey            string    `ddb:",hash-key"`
	RangeKey           string    `ddb:",range-key"`
	IgnoredKey         string    `ddb:"dropMe,hash-key,ignore"`
	BadKey             string    `ddb:"bad,hash-key,range-key"`
	ExpireOn           time.Time `ddb:",expire"`
	BadTag             string    `ddb:",bad-tag"`
	NamedField         string    `ddb:"RenamedField,hash-key"`
	GsiMember          string    `ddb-gsi:"gsi1 hash-key, gsi2 range-key"`
}

func TestDdbRepo_newFieldSpec(t *testing.T) {
	fields := make(map[string]*reflect.StructField)
	listStructFieldsCbk(&testStruct{}, func(fldNum int, field *reflect.StructField, value *reflect.Value) error {
		fields[field.Name] = field
		return nil
	})
	type args struct {
		props *props
		field *reflect.StructField
	}
	tests := []struct {
		name    string
		args    args
		want    *fieldSpec
		wantErr bool
	}{
		{
			name: "privateField should be ignored",
			args: args{
				props: &props{true, true},
				field: fields["privateField"],
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "privateTaggedField should fail",
			args: args{
				props: &props{true, true},
				field: fields["privateTaggedField"],
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "PublicField should result lower case if requested",
			args: args{
				props: &props{true, true},
				field: fields["PublicField"],
			},
			want:    &fieldSpec{name: "publicField"},
			wantErr: false,
		},
		{
			name: "PublicField should result upper case if requested",
			args: args{
				props: &props{true, false},
				field: fields["PublicField"],
			},
			want:    &fieldSpec{name: "PublicField"},
			wantErr: false,
		},
		{
			name: "PublicField should be ignored if props set so",
			args: args{
				props: &props{false, true},
				field: fields["PublicField"],
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "PublicIgnoredField should be ignored if tagged so",
			args: args{
				props: &props{true, true},
				field: fields["PublicIgnoredField"],
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "HashKey should result upper case hash key",
			args: args{
				props: &props{true, false},
				field: fields["HashKey"],
			},
			want:    &fieldSpec{name: "HashKey", isHashKey: true},
			wantErr: false,
		},
		{
			name: "RangeKey should result upper case range key, since it has tags",
			args: args{
				props: &props{true, true},
				field: fields["RangeKey"],
			},
			want:    &fieldSpec{name: "RangeKey", isRangeKey: true},
			wantErr: false,
		},
		{
			name: "IgnoredKey should be ignored as ignore prevails",
			args: args{
				props: &props{true, false},
				field: fields["IgnoredKey"],
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "BadKey should fail",
			args: args{
				props: &props{true, false},
				field: fields["BadKey"],
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "ExpireOn should be ttl field",
			args: args{
				props: &props{true, false},
				field: fields["ExpireOn"],
			},
			want:    &fieldSpec{name: "ExpireOn", isTtlField: true},
			wantErr: false,
		},
		{
			name: "BadTag should fail",
			args: args{
				props: &props{true, false},
				field: fields["BadTag"],
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "NamedField should be renamed and not lower cased",
			args: args{
				props: &props{true, true},
				field: fields["NamedField"],
			},
			want:    &fieldSpec{name: "RenamedField", isHashKey: true},
			wantErr: false,
		},
		{
			name: "GsiMember has two GSI items",
			args: args{
				props: &props{true, true},
				field: fields["GsiMember"],
			},
			want: &fieldSpec{
				name: "gsiMember",
				gsiHash: map[string]bool{
					"gsi1": true,
					"gsi2": false,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newFieldSpec(tt.args.props, tt.args.field)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseFieldSpec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseFieldSpec() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_FieldName(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "it works",
			fields: fields{
				name: "the name",
			},
			want: "the name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.FieldName(); got != tt.want {
				t.Errorf("FieldName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_IsHashKey(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "true for hash key",
			fields: fields{
				isHashKey: true,
			},
			want: true,
		},
		{
			name: "false for range key",
			fields: fields{
				isRangeKey: true,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.IsHashKey(); got != tt.want {
				t.Errorf("IsHashKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_IsKey(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "true for range key",
			fields: fields{
				isRangeKey: true,
			},
			want: true,
		},
		{
			name: "true for hash key",
			fields: fields{
				isHashKey: true,
			},
			want: true,
		},
		{
			name:   "false if no key set",
			fields: fields{},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.IsKey(); got != tt.want {
				t.Errorf("IsKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_IsRangeKey(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "true for range key",
			fields: fields{
				isRangeKey: true,
			},
			want: true,
		},
		{
			name: "false for hash key",
			fields: fields{
				isHashKey: true,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.IsRangeKey(); got != tt.want {
				t.Errorf("IsRangeKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_IsRequired(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "true for required field",
			fields: fields{
				required: true,
			},
			want: true,
		},
		{
			name:   "false for non required field",
			fields: fields{},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.IsRequired(); got != tt.want {
				t.Errorf("IsRequired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_fieldSpec_IsTtlField(t *testing.T) {
	type fields struct {
		name       string
		required   bool
		isHashKey  bool
		isRangeKey bool
		isTtlField bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "true for ttl field",
			fields: fields{
				isTtlField: true,
			},
			want: true,
		},
		{
			name:   "false for non ttl field",
			fields: fields{},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := fieldSpec{
				name:       tt.fields.name,
				required:   tt.fields.required,
				isHashKey:  tt.fields.isHashKey,
				isRangeKey: tt.fields.isRangeKey,
				isTtlField: tt.fields.isTtlField,
			}
			if got := s.IsTtlField(); got != tt.want {
				t.Errorf("IsTtlField() = %v, want %v", got, tt.want)
			}
		})
	}
}
