package config

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/infrawatch/apputils/logging"
)

//Section holds configuration for appropriate section in config file.
type Section struct {
	Options map[string]*Option
}

//Option holds single value of single configuration option.
type Option struct {
	value interface{}
}

//GetString returns option value as string.
func (opt *Option) GetString() string {
	val := reflect.ValueOf(opt.value)
	return val.String()
}

//GetBytes returns option value as slice of bytes.
func (opt *Option) GetBytes() []byte {
	val := reflect.ValueOf(opt.value)
	switch reflect.TypeOf(opt.value).Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		return val.Bytes()
	default:
		return []byte(val.String())
	}
}

//GetStrings returns option value as slice of strings. Use this for value in form of <separator> separated list of strings.
func (opt *Option) GetStrings(separator string) []string {
	val := reflect.ValueOf(opt.value)
	return strings.Split(val.String(), separator)
}

//GetInt returns option value as int.
func (opt *Option) GetInt() int64 {
	val := reflect.ValueOf(opt.value)
	return val.Int()
}

//GetInts returns option value as slice of ints. Use this for value in form of <separator> separated list of ints.
func (opt *Option) GetInts(separator string) []int {
	val := reflect.ValueOf(opt.value)
	items := strings.Split(val.String(), separator)
	output := make([]int, len(items), len(items))
	for idx, opt := range items {
		output[idx], _ = strconv.Atoi(opt)
	}
	return output
}

//GetFLoat returns option value as float.
func (opt *Option) GetFloat() float64 {
	val := reflect.ValueOf(opt.value)
	return val.Float()
}

//GetBool returns option value as bool.
func (opt *Option) GetBool() bool {
	val := reflect.ValueOf(opt.value)
	return val.Bool()
}

func (opt *Option) GetStructured() interface{} {
	return opt.value
}

//Config interface for methods to accept various types of config objects (INI/JSON/...)
type Config interface {
	Parse(path string) error
	GetOption(name string) (*Option, error)
}

//Validator checks the validity of the config option value. It accepts single value
//and returns nil (and corrected value if possible) if the value is valid
//or appropriate error otherwise.
type Validator func(interface{}) (interface{}, error)

//Parameter holds metadata and validators for the single configuration option.
type Parameter struct {
	Name       string
	Tag        string
	Default    interface{}
	Validators []Validator
}

//WithConfigBase holds config metadata and logger
type WithConfigBase struct {
	log      *logging.Logger
	metadata map[string][]Parameter
	Sections map[string]*Section
}

//GetMetadata returns config metadata
func (base *WithConfigBase) GetMetadata() map[string][]Parameter {
	output := make(map[string][]Parameter)
	for k, v := range base.metadata {
		output[k] = v
	}
	return output
}

func validate(value interface{}, validators []Validator) (interface{}, error) {
	var err error
	val := value
	for _, validator := range validators {
		val, err = validator(val)
		if err != nil {
			return nil, err
		}
	}
	return val, nil
}

func createOption(value interface{}, metadata Parameter, log *logging.Logger) (*Option, error) {
	option := &Option{}
	val := reflect.ValueOf(value)
	result := ""
	if !val.IsValid() {
		value = metadata.Default
		result = "default"
	} else {
		result = "parsed"
	}
	value, err := validate(value, metadata.Validators)
	log.Metadata(map[string]interface{}{
		"parameter": metadata.Name,
		"value":     fmt.Sprintf("%v", value),
		"result":    result,
	})
	if err == nil {
		option.value = value
		log.Debug("using configuration value.")
	} else {
		err = fmt.Errorf("failed validation for parameter %s: %s", metadata.Name, err)
		log.Error("failed parameter validation.")
	}
	return option, err
}
