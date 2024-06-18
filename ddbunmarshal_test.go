package ddbrepo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/rotmistrk/secrets/lib/must"
	"reflect"
	"testing"
	"time"
)

type mockUnmarshalStruct struct {
	Name    string
	Count   int
	Numbers map[string]float64
	Expire  time.Time `ddb:"expire, expire"`
}

func TestUnmarshal(t *testing.T) {
	type args struct {
		props  parseProps
		target interface{}
		item   map[string]types.AttributeValue
	}
	when := must.Must(time.Parse(time.RFC3339, "2022-01-01T00:00:00Z"))
	props := &props{true, true}
	sample := mockUnmarshalStruct{
		Name:  "a-name",
		Count: 42,
		Numbers: map[string]float64{
			"pi":     3.14159,
			"golden": 1.61,
		},
		Expire: when,
	}
	sampleCopy := mockUnmarshalStruct{}
	smapleAv, _ := Marshal(props, &sample)
	tests := []struct {
		name    string
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "marshalled unmarshalls",
			args: args{
				props:  props,
				target: &sampleCopy,
				item:   smapleAv,
			},
			want:    &sample,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Unmarshal(tt.args.props, tt.args.target, tt.args.item); (err != nil) != tt.wantErr {
				t.Errorf("Unmarshal() error = %v, wantErr %v", err, tt.wantErr)
			} else if !reflect.DeepEqual(tt.args.target, tt.want) {
				t.Errorf("Unmarshalled value differs from source:\n\t%v\n\t%v", tt.args.target, tt.want)
			}
		})
	}
}
