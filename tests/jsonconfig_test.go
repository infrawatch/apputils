package tests

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
	"github.com/stretchr/testify/assert"
)

type InnerTestObject struct {
	Type string `json:"type"`
	URL  string `json:"url"`
}

type OuterTestObject struct {
	Test        string            `json:"test"`
	Connections []InnerTestObject `json:"data_sources"`
}

var (
	JSONConfigContent = `{
		"Default": {
			"log_file": "/var/log/another.log",
			"NoTag": "woot?",
			"log_level": "DEBUG",
			"port": 1234
		},
		"Amqp1": {
			"float": 5.5,
			"connections": {
				"test": "woobalooba",
				"data_sources": [
				  {"type": "test1", "url": "booyaka"},
				  {"type": "test2", "url": "foobar"}
			  ]
		  }
		}
	}
`
	JSONConfigMetadata = map[string][]config.Parameter{
		"Default": []config.Parameter{
			config.Parameter{Name: "LogFile", Tag: `json:"log_file"`, Default: "/var/log/the.log", Validators: []config.Validator{}},
			config.Parameter{Name: "NoTag", Tag: "", Default: "notag", Validators: []config.Validator{}},
			config.Parameter{Name: "LogLevel", Tag: `json:"log_level"`, Default: "INFO", Validators: []config.Validator{config.StringOptionsValidatorFactory([]string{"DEBUG", "INFO", "WARNING", "ERROR"})}},
			config.Parameter{Name: "AllowExec", Tag: `json:"allow_exec"`, Default: true, Validators: []config.Validator{config.BoolValidatorFactory()}},
			config.Parameter{Name: "Port", Tag: `json:"port"`, Default: 5666, Validators: []config.Validator{config.IntValidatorFactory()}},
		},
		"Amqp1": []config.Parameter{
			config.Parameter{Name: "Float", Tag: `json:"float"`, Default: 6.6, Validators: []config.Validator{}},
		},
	}
)

func TestJSONConfigValues(t *testing.T) {
	// create temporary config file
	tmpdir, err := ioutil.TempDir(".", "config_test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	logpath := path.Join(tmpdir, "test.log")
	file, err := ioutil.TempFile(tmpdir, "test.conf")
	if err != nil {
		t.Fatal(err)
	}
	// save test content
	file.WriteString(JSONConfigContent)
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	log, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		fmt.Printf("Failed to open log file %s.\n", logpath)
		os.Exit(2)
	}
	defer log.Destroy()

	t.Run("Test parsed flat values from JSON configuration file", func(t *testing.T) {
		conf := config.NewJSONConfig(JSONConfigMetadata, log)
		err = conf.Parse(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		// test parsed sections
		sections := []string{}
		for key := range conf.Sections {
			sections = append(sections, key)
		}
		assert.ElementsMatch(t, []string{"Default", "Amqp1"}, sections)
		// test parsed overrided values
		assert.Equal(t, "/var/log/another.log", conf.Sections["Default"].Options["LogFile"].GetString(), "Did not parse correctly")
		assert.Equal(t, "DEBUG", conf.Sections["Default"].Options["LogLevel"].GetString(), "Did not parse correctly")
		assert.Equal(t, true, conf.Sections["Default"].Options["AllowExec"].GetBool(), "Did not parse correctly")
		assert.Equal(t, int64(1234), conf.Sections["Default"].Options["Port"].GetInt(), "Did not parse correctly")
		assert.Equal(t, float64(5.5), conf.Sections["Amqp1"].Options["Float"].GetFloat(), "Did not parse correctly")
	})

	/*
		t.Run("Test parsed structured values from JSON configuration file", func(t *testing.T) {
			conf := config.NewJSONConfig(JSONConfigMetadata, log)

			var connections OuterTestObject
			conf.AddStructured("Amqp1", "Connections", `json:"connections"`, connections)

			err = conf.Parse(file.Name())
			if err != nil {
				t.Fatal(err)
			}
			// test parsed overrided values
			connObj := conf.Sections["Amqp1"].Options["Connections"].GetStructured()
			fmt.Printf("-----%T <> %v\n", connObj, connObj)

			//assert.Equal(t, "woobalooba", connObj.(*OuterTestObject).Test, "Did not parse correctly")
		})*/
}
