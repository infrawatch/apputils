package tests

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/paramite/collectd-sensubility/config"
	"github.com/paramite/collectd-sensubility/logging"
	"github.com/stretchr/testify/assert"
)

var CONFIG_CONTENT = `
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

type validatorTest struct {
	parameter string
	validator config.Validator
	defValue  string
}

func TestConfigValues(t *testing.T) {
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
	file.WriteString(CONFIG_CONTENT)
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

	metadata := config.GetAgentConfigMetadata()
	conf, err := config.NewConfig(metadata, log)
	if err != nil {
		t.Fatal(err)
	}
	err = conf.Parse(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	// test parsed sections
	sections := []string{}
	for key, _ := range conf.Sections {
		sections = append(sections, key)
	}
	assert.ElementsMatch(t, []string{"default", "sensu", "amqp1"}, sections)
	// test default values
	assert.Equal(t, []string{"all", "default"}, conf.Sections["sensu"].Options["subscriptions"].GetStrings(","))
	// test parsed overrided values
	assert.Equal(t, "/var/tmp/test.log", conf.Sections["default"].Options["log_file"].GetString(), "Did not parse correctly")
	assert.Equal(t, false, conf.Sections["default"].Options["allow_exec"].GetBool(), "Did not parse correctly")
	assert.Equal(t, 666, conf.Sections["amqp1"].Options["port"].GetInt(), "Did not parse correctly")
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
	file.WriteString(CONFIG_CONTENT)
	err = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	// test failing parsing (each validator separately)
	tests := []validatorTest{
		validatorTest{"IntValidator", config.IntValidatorFactory(), "3"},
		validatorTest{"MultiIntValidator", config.MultiIntValidatorFactory(","), "1,2"},
		validatorTest{"BoolValidator", config.BoolValidatorFactory(), "true"},
		validatorTest{"OptionsValidator", config.OptionsValidatorFactory([]string{"bar", "baz"}), "bar"},
	}
	for _, test := range tests {
		metadata := map[string][]config.Parameter{
			"invalid": []config.Parameter{
				config.Parameter{test.parameter, test.defValue, []config.Validator{test.validator}},
			},
		}
		conf, err := config.NewConfig(metadata, log)
		err = conf.Parse(file.Name())
		if err == nil {
			t.Errorf("Failed to report validation error with %s.", test.parameter)
		}
	}
	// test failing constructor (validation of default values)
	metadata := map[string][]config.Parameter{
		"invalid": []config.Parameter{
			config.Parameter{"default_test", "default", []config.Validator{config.IntValidatorFactory()}},
		},
	}
	_, err = config.NewConfig(metadata, log)
	if err == nil {
		t.Errorf("Failed to report validation error in constructor.")
	}
	os.Remove(file.Name())
}
