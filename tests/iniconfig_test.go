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

var IniConfigContent = `
[default]
log_file=/var/tmp/test.log
allow_exec=false

[amqp1]
port=666

[invalid]
IntValidator=whoops
MultiIntValidator=1,2,whoops,4
BoolValidator=no-way
OptionsValidator=foo
`

type ValidatorTest struct {
	Parameter string
	Validator config.Validator
	defValue  string
}

func TestINIConfigValues(t *testing.T) {
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
	file.WriteString(IniConfigContent)
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	// test parsing
	log, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		fmt.Printf("Failed to open log file %s.\n", logpath)
		os.Exit(2)
	}
	defer log.Destroy()

	metadata := map[string][]config.Parameter{
		"default": {
			{Name: "log_file", Tag: "", Default: "/var/log/collectd-sensubility.log", Validators: []config.Validator{}},
			{Name: "log_level", Tag: "", Default: "INFO", Validators: []config.Validator{config.StringOptionsValidatorFactory([]string{"DEBUG", "INFO", "WARNING", "ERROR"})}},
			{Name: "allow_exec", Tag: "", Default: true, Validators: []config.Validator{config.BoolValidatorFactory()}},
		},
		"amqp1": {
			{Name: "host", Tag: "", Default: "localhost", Validators: []config.Validator{}},
			{Name: "port", Tag: "", Default: 5666, Validators: []config.Validator{config.IntValidatorFactory()}},
			{Name: "user", Tag: "", Default: "guest", Validators: []config.Validator{}},
			{Name: "password", Tag: "", Default: "guest", Validators: []config.Validator{}},
		},
	}
	conf := config.NewINIConfig(metadata, log)
	err = conf.Parse(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	// test parsed sections
	sections := []string{}
	for key := range conf.Sections {
		sections = append(sections, key)
	}
	assert.ElementsMatch(t, []string{"default", "amqp1"}, sections)
	// test parsed overrided values
	assert.Equal(t, "/var/tmp/test.log", conf.Sections["default"].Options["log_file"].GetString(), "Did not parse correctly")
	assert.Equal(t, false, conf.Sections["default"].Options["allow_exec"].GetBool(), "Did not parse correctly")
	assert.Equal(t, int64(666), conf.Sections["amqp1"].Options["port"].GetInt(), "Did not parse correctly")
	os.Remove(file.Name())
}

func TestValidators(t *testing.T) {
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

	log, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		fmt.Printf("Failed to open log file %s.\n", logpath)
		os.Exit(2)
	}
	defer log.Destroy()

	// save test content
	file.WriteString(IniConfigContent)
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test parsed values from INI configuration file", func(t *testing.T) {
		tests := []ValidatorTest{
			{"IntValidator", config.IntValidatorFactory(), "3"},
			{"MultiIntValidator", config.MultiIntValidatorFactory(","), "1,2"},
			{"BoolValidator", config.BoolValidatorFactory(), "true"},
			{"OptionsValidator", config.StringOptionsValidatorFactory([]string{"bar", "baz"}), "bar"},
		}
		for _, test := range tests {
			metadata := map[string][]config.Parameter{
				"invalid": {
					{Name: test.Parameter, Tag: "", Default: test.defValue, Validators: []config.Validator{test.Validator}},
				},
			}
			conf := config.NewINIConfig(metadata, log)
			err = conf.Parse(file.Name())
			if err == nil {
				t.Errorf("Failed to report validation error with %s.", test.Parameter)
			}
		}
	})

	t.Run("Test of raising validation errors", func(t *testing.T) {
		metadata := map[string][]config.Parameter{
			"invalid": {
				{Name: "default_test", Tag: "", Default: "default", Validators: []config.Validator{config.IntValidatorFactory()}},
			},
		}
		conf := config.NewINIConfig(metadata, log)
		if err = conf.Parse(file.Name()); err == nil {
			t.Errorf("Failed to report validation error in constructor.")
		}
	})

	t.Run("Test of fetching option dynamically", func(t *testing.T) {
		metadata := map[string][]config.Parameter{
			"default": {
				{Name: "log_file", Tag: "", Default: "/var/log/collectd-sensubility.log", Validators: []config.Validator{}},
			},
		}
		conf := config.NewINIConfig(metadata, log)
		err = conf.Parse(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		if opt, err := conf.GetOption("default/log_file"); err != nil {
			t.Errorf("Failed to find existing option according to addr string: %s\n", err)
		} else {
			assert.Equal(t, "/var/tmp/test.log", opt.GetString(), "Did not parse correctly")
		}
	})

	os.Remove(file.Name())
}
