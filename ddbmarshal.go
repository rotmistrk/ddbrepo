package ddbrepo

import (
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"reflect"
)

func Marshal(props parseProps, source interface{}) (result map[string]types.AttributeValue, err error) {
	return MarshalTagFilter(props, source, IncludeAll, "")
}

func MarshalKey(props parseProps, source interface{}, prefix string) (result map[string]types.AttributeValue, err error) {
	return MarshalTagFilter(props, source, IncludeKey, prefix)
}

func makeListStructFieldsCbk(props parseProps, attrValueMap map[string]types.AttributeValue, filter func(spec fieldSpec) bool, prefix string) listStructFieldsCallback {
	return func(fldNum int, field *reflect.StructField, value *reflect.Value) error {
		spec, err := newFieldSpec(props, field)
		if err == nil && spec != nil && filter(*spec) {
			attrValueMap[prefix+spec.name], err = attributevalue.Marshal(value.Interface())
		}
		return err
	}
}

func IncludeAll(spec fieldSpec) bool {
	return true
}

func IncludeKey(spec fieldSpec) bool {
	return spec.IsKey()
}

func IncludeVersion(spec fieldSpec) bool {
	return spec.IsVersionField()
}

func AnyOfFilters(filters ...func(spec fieldSpec) bool) func(spec fieldSpec) bool {
	return func(spec fieldSpec) bool {
		for _, f := range filters {
			if f(spec) {
				return true
			}
		}
		return false
	}
}

func AllOfFilters(filters ...func(spec fieldSpec) bool) func(spec fieldSpec) bool {
	return func(spec fieldSpec) bool {
		for _, f := range filters {
			if !f(spec) {
				return false
			}
		}
		return true
	}
}

func MarshalTagFilter(props parseProps, source interface{}, filter func(spec fieldSpec) bool, prefix string) (result map[string]types.AttributeValue, err error) {
	result = make(map[string]types.AttributeValue)
	err = listStructFieldsCbk(source, makeListStructFieldsCbk(props, result, filter, prefix))
	if err != nil {
		return nil, err
	} else {
		return result, err
	}
}
