package ddbrepo

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

func Unmarshal(props parseProps, target interface{}, item map[string]types.AttributeValue) error {
	callback := makeUnmarshalCallback(props, target, item)
	if err := listStructFieldsCbk(target, callback); err != nil {
		return err
	} else {
		return nil
	}
}

func makeUnmarshalCallback(props parseProps, target interface{}, item map[string]types.AttributeValue) func(pos int, field *reflect.StructField, value *reflect.Value) error {
	return func(pos int, field *reflect.StructField, value *reflect.Value) error {
		if spec, err := newFieldSpec(props, field); err != nil {
			return err
		} else if mapEntry, found := item[spec.name]; found {
			// dest := value.Interface()
			destination := value.Addr().Interface()
			if err := attributevalue.Unmarshal(mapEntry, destination); err != nil {
				return err
			}
		}
		return nil
	}

}
