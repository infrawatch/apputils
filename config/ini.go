package config

import (
	"fmt"
	"strings"

	"github.com/go-ini/ini"
	"github.com/infrawatch/apputils/logging"
)

//INIConfig holds complete configuration data and metadata for configuration saved as INI file.
type INIConfig struct {
	WithConfigBase
}

//NewINIConfig creates and initializes new INIConfig object according to given metadata
func NewINIConfig(metadata map[string][]Parameter, logger *logging.Logger) *INIConfig {
	return &INIConfig{
		WithConfigBase: WithConfigBase{log: logger, metadata: metadata, Sections: make(map[string]*Section)},
	}
}

//Parse loads data from given file
func (conf INIConfig) Parse(path string) error {
	options := ini.LoadOptions{
		AllowPythonMultilineValues: true,
		IgnoreInlineComment:        true,
	}
	data, err := ini.LoadSources(options, path)
	if err != nil {
		return err
	}
	for sectionName, sectionMetadata := range conf.metadata {
		conf.Sections[sectionName] = &Section{Options: make(map[string]*Option)}
		if sectionData, err := data.GetSection(sectionName); err == nil {
			for _, param := range sectionMetadata {
				var value interface{}
				if paramData, err := sectionData.GetKey(param.Name); err == nil {
					value = paramData.Value()
				} else {
					value = nil
				}
				if opt, err := createOption(value, param, conf.log); err == nil {
					conf.Sections[sectionName].Options[param.Name] = opt
				} else {
					return err
				}
			}
		} else {
			// default values for the whole section
			for _, param := range sectionMetadata {
				if opt, err := createOption(nil, param, conf.log); err == nil {
					conf.Sections[sectionName].Options[param.Name] = opt
				} else {
					return err
				}
			}
		}
	}
	return nil
}

//GetOption returns Option objects according to given "section-name/option-name" string.
func (conf INIConfig) GetOption(name string) (*Option, error) {
	var option *Option
	var err error

	nameparts := strings.SplitN(name, "/", 2)
	if section, ok := conf.Sections[nameparts[0]]; ok {
		if opt, ok := section.Options[nameparts[1]]; ok {
			option = opt
		} else {
			err = fmt.Errorf("did not find option '%s' in section '%s'", nameparts[1], nameparts[0])
		}
	} else {
		err = fmt.Errorf("did not find section '%s'", nameparts[0])
	}
	return option, err
}
