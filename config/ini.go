package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/go-ini/ini"
	"github.com/infrawatch/apputils/logging"
)

const (
	DEFAULT_HOSTNAME = "localhost.localdomain"
	DEFAULT_IP       = "127.0.0.1"
)

/******************************************************************************/
type Validator func(string) error

type Parameter struct {
	Name       string
	Default    string
	Validators []Validator
}

type Option struct {
	value string
}

type Section struct {
	Options map[string]*Option
}

type IniConfig struct {
	log      *logging.Logger
	metadata map[string][]Parameter
	Sections map[string]*Section
}

/******************************************************************************/
func OptionsValidatorFactory(options []string) Validator {
	return func(input string) error {
		for _, option := range options {
			if input == option {
				return nil
			}
		}
		return fmt.Errorf("Value (%v) is not one of allowed options: %v", input, options)
	}
}

func BoolValidatorFactory() Validator {
	return func(input string) error {
		_, err := strconv.ParseBool(input)
		return err
	}
}

func IntValidatorFactory() Validator {
	return func(input string) error {
		_, err := strconv.Atoi(input)
		return err
	}
}

func MultiIntValidatorFactory(separator string) Validator {
	return func(input string) error {
		for _, item := range strings.Split(input, separator) {
			_, err := strconv.Atoi(item)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

/********** Value methods ***********/
func (opt *Option) GetString() string {
	return opt.value
}

func (opt *Option) GetBytes() []byte {
	return []byte(opt.value)
}

func (opt *Option) GetStrings(separator string) []string {
	return strings.Split(opt.value, separator)
}

func (opt *Option) GetInt() int {
	output, _ := strconv.Atoi(opt.value)
	return output
}

func (opt *Option) GetInts(separator string) []int {
	options := strings.Split(opt.value, separator)
	output := make([]int, len(options), len(options))
	for idx, opt := range options {
		output[idx], _ = strconv.Atoi(opt)
	}
	return output
}

func (opt *Option) GetBool() bool {
	output, _ := strconv.ParseBool(opt.value)
	return output
}

/** Config methods and fungtions **/
func validate(value string, validators []Validator) error {
	for _, validator := range validators {
		//fmt.Printf("%v - %v\n", value, validator(value))
		err := validator(value)
		if err != nil {
			return fmt.Errorf("Invalid value: %v", value)
		}
	}
	return nil
}

func NewConfig(metadata map[string][]Parameter, logger *logging.Logger) (*IniConfig, error) {
	var conf IniConfig
	conf.metadata = metadata
	conf.log = logger
	// initialize config with default values
	conf.Sections = make(map[string]*Section)
	for sectionName, sectionMetadata := range conf.metadata {
		sect := Section{}
		sect.Options = make(map[string]*Option)
		conf.Sections[sectionName] = &sect
		for _, param := range sectionMetadata {
			if err := validate(param.Default, param.Validators); err != nil {
				return nil, fmt.Errorf("Failed to validate parameter %s. %s", param.Name, err.Error())
			}
			opt := Option{param.Default}
			sect.Options[param.Name] = &opt
		}
	}
	return &conf, nil
}

//Parse loads data from given file
func (conf *IniConfig) Parse(path string) error {
	data, err := ini.LoadSources(ini.LoadOptions{AllowPythonMultilineValues: true}, path)
	if err != nil {
		return err
	}
	for sectionName, sectionMetadata := range conf.metadata {
		if sectionData, err := data.GetSection(sectionName); err == nil {
			for _, param := range sectionMetadata {
				if paramData, err := sectionData.GetKey(param.Name); err == nil {
					if err := validate(paramData.Value(), param.Validators); err != nil {
						return fmt.Errorf("Failed to validate parameter %s. %s", param.Name, err.Error())
					}
					conf.Sections[sectionName].Options[param.Name].value = paramData.Value()
					conf.log.Metadata(map[string]interface{}{
						"section": sectionName,
						"option":  param.Name,
						"value":   paramData.Value(),
					})
					conf.log.Debug("Using parsed configuration value.")
				} else {
					conf.log.Metadata(map[string]interface{}{
						"section": sectionName,
						"option":  param.Name,
						"value":   conf.Sections[sectionName].Options[param.Name].value,
					})
					conf.log.Debug("Using default configuration value.")
				}
			}
		}
	}
	return nil
}
