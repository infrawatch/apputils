package config

//JSONConfig holds complete configuration data and metadata for configuration saved as JSON file.
type JSONConfig struct {
	WithConfigBase
	structure interface{}
	Options   map[string]*Option
}

/*
//NewJSONConfig creates and initializes new config object according to given metadata.
func NewJSONConfig(metadata map[string][]Parameter, logger *logging.Logger) *JSONConfig {
  conf := &JSONConfig{}
  // create structure for JSON unmarshalling
	sections := []reflect.StructField{}
	for section, params := range metadata {
		optFields := []reflect.StructField{}
		for _, p := range params {
			optFields = append(optFields,
				reflect.StructField{
					Name: p.Name,
					Type: reflect.TypeOf(p.Default),
					Tag:  reflect.StructTag(p.Tag),
				}
			)
		}
		sections = append(sections,
			reflect.StructField{
				Name: section,
				Type: reflect.TypeOf(reflect.New(reflect.StructOf(optFields))),
			}
	  )
  }
	conf.structure = reflect.New(reflect.StructOf(sections))

	return &JSONConfig{
		WithConfigBase: WithConfigBase{log: logger, metadata: metadata, Sections: make(map[string]*Section)},
		structure: reflect.New(reflect.StructOf(sections))
	}
}





	err := json.Unmarshal(data, structure)
	return &JSONConfig{
		WithConfigBase: WithConfigBase{log: logger, metadata: metadata},
	  structure: structure,
	}
	return &conf, nil
}

//Parse loads data from given file
func (conf *JSONConfigWithValidation) Parse(path string) error {

	err := json.Unmarshal(data, &conf)




	return nil
}
*/
