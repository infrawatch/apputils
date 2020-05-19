package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

//StringOptionsValidatorFactory creates validator for checking if the validator's given value is one of the factory's given string options.
func StringOptionsValidatorFactory(options []string) Validator {
	return func(input interface{}) (interface{}, error) {
		if val, ok := input.(string); ok {
			for _, option := range options {
				if val == option {
					return val, nil
				}
			}
		}
		return nil, fmt.Errorf("value (%v) is not one of allowed options: %v", input, options)
	}
}

//BoolValidatorFactory creates validator for checking if the validator's given value is bool.
func BoolValidatorFactory() Validator {
	return func(input interface{}) (interface{}, error) {
		switch reflect.TypeOf(input).Kind() {
		case reflect.Bool:
			return input, nil
		case reflect.String:
			val, err := strconv.ParseBool(input.(string))
			if err == nil {
				return val, nil
			}
			fallthrough
		default:
			return nil, fmt.Errorf("value (%v) is not bool", input)
		}
	}
}

//IntValidatorFactory creates validator for checking if the validator's given value is int.
func IntValidatorFactory() Validator {
	return func(input interface{}) (interface{}, error) {
		switch reflect.TypeOf(input).Kind() {
		case reflect.Int:
			fallthrough
		case reflect.Int8:
			fallthrough
		case reflect.Int16:
			fallthrough
		case reflect.Int32:
			fallthrough
		case reflect.Int64:
			return reflect.ValueOf(input).Elem(), nil
		case reflect.Float64:
			return int64(input.(float64)), nil
		case reflect.String:
			val, err := strconv.ParseInt(input.(string), 10, 64)
			if err == nil {
				return val, nil
			}
			fallthrough
		default:
			return nil, fmt.Errorf("value (%v) is not int", input)
		}
	}
}

//MultiIntValidatorFactory creates validator for checking if the validator's given value
//is list of ints separated by given separator
func MultiIntValidatorFactory(separator string) Validator {
	return func(input interface{}) (interface{}, error) {
		switch reflect.TypeOf(input).Kind() {
		case reflect.Array:
			fallthrough
		case reflect.Slice:
			out := make([]int, 0)
			slc := reflect.ValueOf(input)
			validator := IntValidatorFactory()
			for i := 0; i < slc.Len(); i++ {
				itm, err := validator(slc.Index(i))
				if err != nil {
					return nil, fmt.Errorf("value (%v) is not list of integers", input)
				}
				out = append(out, itm.(int))
			}
			return out, nil
		case reflect.String:
			out := make([]int, 0)
			for _, item := range strings.Split(input.(string), separator) {
				itm, err := strconv.Atoi(item)
				if err != nil {
					return nil, err
				}
				out = append(out, itm)
			}
			return out, nil
		default:
			return nil, fmt.Errorf("value (%v) is not list of integers", input)
		}
	}
}

//TODO: Add more builtin validators
