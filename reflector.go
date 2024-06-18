package ddbrepo

import (
	"errors"
	"fmt"
	"reflect"
)

func getValidMarshallingTargetValue(target interface{}) (val reflect.Value, err error) {
	defer func() {
		if r := recover(); r != nil {
			val = reflect.Value{}
			err = errors.New(fmt.Sprintf("impossible to (un)marshal to %t: %v", target, r))
		}
	}()
	targetValue := reflect.ValueOf(target).Elem()
	if targetValue.Kind() == reflect.Ptr {
		targetValue = reflect.Indirect(targetValue)
	}
	if targetValue.Kind() != reflect.Struct || !targetValue.CanAddr() {
		return reflect.Value{}, errors.New("struct pointer expected, " + targetValue.Kind().String() + " receved")
	}
	return targetValue, nil
}
