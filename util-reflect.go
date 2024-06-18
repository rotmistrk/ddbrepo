package ddbrepo

import (
	"errors"
	"fmt"
	"reflect"
)

type listStructFieldsCallback func(fldNum int, field *reflect.StructField, value *reflect.Value) error

func listStructFieldsCbk(target interface{}, cbk listStructFieldsCallback) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New(fmt.Sprintf("impossible to (un)marshal to %t: %v", target, r))
		}
	}()
	targetValue := reflect.ValueOf(target).Elem()
	if targetValue.Kind() == reflect.Ptr {
		targetValue = reflect.Indirect(targetValue)
	}
	if targetValue.Kind() != reflect.Struct || !targetValue.CanAddr() {
		return errors.New("struct pointer expected, " + targetValue.Kind().String() + " receved")
	}
	targetType := targetValue.Type()
	for i, I := 0, targetValue.NumField(); i < I; i++ {
		field := targetType.Field(i)
		value := targetValue.Field(i)
		if err = cbk(i, &field, &value); err != nil {
			return
		}
	}
	return
}
