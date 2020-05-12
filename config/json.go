package config

import (
	"encoding/json"
	"io/ioutil"
	"reflect"

	"github.com/infrawatch/apputils/logging"
)

//JSONConfig holds complete configuration data and metadata for configuration saved as JSON file.
type JSONConfig struct {
	WithConfigBase
	flat       interface{}
	structured map[string][]reflect.StructField
}

//NewJSONConfig creates and initializes new config object according to given metadata.
func NewJSONConfig(metadata map[string][]Parameter, logger *logging.Logger) *JSONConfig {
	// create flat structure for JSON unmarshalling
	sections := []reflect.StructField{}
	for section, params := range metadata {
		optFields := []reflect.StructField{}
		for _, p := range params {
			optFields = append(optFields,
				reflect.StructField{
					Name: p.Name,
					Type: reflect.TypeOf(p.Default),
					Tag:  reflect.StructTag(p.Tag),
				},
			)
		}
		sections = append(sections,
			reflect.StructField{
				Name: section,
				Type: reflect.TypeOf(reflect.New(reflect.StructOf(optFields))),
			},
		)
	}

	conf := JSONConfig{
		WithConfigBase: WithConfigBase{log: logger, metadata: metadata, Sections: make(map[string]*Section)},
		flat:           reflect.New(reflect.StructOf(sections)).Elem(),
		structured:     make(map[string][]reflect.StructField),
	}
	return &conf
}

//AddStructured can be used when config values are structured deeper than section/parameter.
//Note that structured parameters are missing validation logic other than through standard json module (can be changed in future)
func (conf *JSONConfig) AddStructured(section, name, tag string, object interface{}) {
	if _, ok := conf.structured[section]; !ok {
		conf.structured[section] = make([]reflect.StructField, 0)
	}
	conf.structured[section] = append(conf.structured[section],
		reflect.StructField{
			Name: name,
			Type: reflect.TypeOf(object),
			Tag:  reflect.StructTag(tag),
		},
	)
}

//Parse loads data from given file
func (conf *JSONConfig) Parse(path string) error {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		conf.log.Metadata(map[string]interface{}{
			"error": err,
			"path":  path,
		})
		conf.log.Error("unable to read provided configuration file")
		return err
	}
	// parse flat parameters
	if err := json.Unmarshal(data, &conf.flat); err != nil {
		conf.log.Metadata(map[string]interface{}{
			"error": err,
		})
		conf.log.Error("unable to parse data from provided configuration file")
		return err
	}

	flat := reflect.Indirect(reflect.ValueOf(&conf.flat)).Elem()
	for section, params := range conf.metadata {
		conf.Sections[section] = &Section{Options: make(map[string]*Option)}
		for _, param := range params {
			sectMap := flat.MapIndex(reflect.ValueOf(section))
			tag := reflect.StructTag(param.Tag).Get("json")
			if tag == "" {
				tag = param.Name
			}
			field := sectMap.Elem().MapIndex(reflect.ValueOf(tag))
			var value interface{}
			if field.IsValid() {
				value = field.Interface()
			}
			if opt, err := createOption(value, param, conf.log); err == nil {
				conf.Sections[section].Options[param.Name] = opt
			} else {
				return err
			}
		}
	}

	// parse structured parameters
	sections := []reflect.StructField{}
	for sect, params := range conf.structured {
		sections = append(sections,
			reflect.StructField{
				Name: sect,
				Type: reflect.StructOf(params),
			},
		)
	}
	parsed := reflect.New(reflect.StructOf(sections)).Interface()

	if err := json.Unmarshal(data, parsed); err != nil {
		conf.log.Metadata(map[string]interface{}{
			"error": err,
		})
		conf.log.Error("unable to parse data from provided configuration file")
		return err
	}

	parsedSections := reflect.ValueOf(parsed).Elem()
	for section, params := range conf.structured {
		if _, ok := conf.Sections[section]; !ok {
			conf.Sections[section] = &Section{Options: make(map[string]*Option)}
		}
		sect := parsedSections.FieldByName(section)
		for _, param := range params {
			conf.Sections[section].Options[param.Name] = &Option{value: sect.FieldByName(param.Name).Interface()}
		}
	}
	return nil
}
