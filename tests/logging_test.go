package tests

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"testing"

	"github.com/paramite/collectd-sensubility/logging"
	"github.com/stretchr/testify/assert"
)

// https://stackoverflow.com/a/51328256
func getLastLineWithSeek(filepath string) (string, error) {
	fileHandle, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer fileHandle.Close()

	line := ""
	var cursor int64 = 0
	stat, _ := fileHandle.Stat()
	filesize := stat.Size()
	for {
		cursor -= 1
		fileHandle.Seek(cursor, io.SeekEnd)

		char := make([]byte, 1)
		fileHandle.Read(char)

		if cursor != -1 && (char[0] == 10 || char[0] == 13) {
			break
		}

		line = fmt.Sprintf("%s%s", string(char), line)
		if cursor == -filesize {
			break
		}
	}

	return line, nil
}

func TestLogger(t *testing.T) {
	// create temporary logging directory
	tmpdir, err := ioutil.TempDir(".", "logging_test")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	logpath := path.Join(tmpdir, "test.log")
	logpath2 := path.Join(tmpdir, "test2.log")
	log, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		t.Fatalf("Failed to create logger: %s", err)
	}

	t.Run("Test DEBUG log level", func(t *testing.T) {
		log.Debug("Test debug 1")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[DEBUG] Test debug 1\n", actual)

		log.Info("Test debug 2")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[INFO] Test debug 2\n", actual)

		log.Warn("Test debug 3")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[WARN] Test debug 3\n", actual)

		log.Error("Test debug 4")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test debug 4\n", actual)
	})

	t.Run("Test INFO log level", func(t *testing.T) {
		log.Level = logging.INFO
		log.Debug("Test info 1")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test debug 4\n", actual)

		log.Info("Test info 2")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[INFO] Test info 2\n", actual)

		log.Warn("Test info 3")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[WARN] Test info 3\n", actual)

		log.Error("Test info 4")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test info 4\n", actual)
	})

	t.Run("Test WARN log level", func(t *testing.T) {
		log.Level = logging.WARN
		log.Debug("Test warn 1")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test info 4\n", actual)

		log.Info("Test warn 2")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test info 4\n", actual)

		log.Warn("Test warn 3")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[WARN] Test warn 3\n", actual)

		log.Error("Test warn 4")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test warn 4\n", actual)
	})

	t.Run("Test ERROR log level", func(t *testing.T) {
		log.Level = logging.ERROR
		log.Debug("Test error 1")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test warn 4\n", actual)

		log.Info("Test error 2")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test warn 4\n", actual)

		log.Warn("Test error 3")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test warn 4\n", actual)

		log.Error("Test error 4")
		actual, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Equal(t, "[ERROR] Test error 4\n", actual)
	})

	t.Run("Test SetFile", func(t *testing.T) {
		log.Level = logging.ERROR

		lastInFile1, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}

		err = log.SetFile(logpath2, 0666)
		if err != nil {
			t.Fatalf("Failed switching log files: %s", err)
		}
		log.Error("Test SetFile")

		actualInFile1, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}

		assert.Equal(t, lastInFile1, actualInFile1)

		actualInFile2, err := getLastLineWithSeek(logpath2)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}

		assert.Equal(t, "[ERROR] Test SetFile\n", actualInFile2)

		// change it back
		err = log.SetFile(logpath, 0666)
		if err != nil {
			t.Fatalf("Failed switching log files: %s", err)
		}
		log.Error("Test SetFile2")

		actualInFile1, err = getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}

		assert.Equal(t, "[ERROR] Test SetFile2\n", actualInFile1)

		actualInFile2, err = getLastLineWithSeek(logpath2)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}

		assert.Equal(t, "[ERROR] Test SetFile\n", actualInFile2)
	})

	t.Run("Test metadata", func(t *testing.T) {
		log.Metadata(map[string]interface{}{"foo": "bar", "baz": []string{"bam", "vam"}})
		log.Error("Test metadata 1")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Regexp(t, `\[ERROR\] Test metadata 1 \[(foo: bar, baz: \[bam vam\]|baz: \[bam vam\], foo: bar)\]`, actual)
	})

	t.Run("Test timestamp", func(t *testing.T) {
		log.Timestamp = true
		log.Error("Test timestamp")
		actual, err := getLastLineWithSeek(logpath)
		if err != nil {
			t.Fatalf("Failed to fetch last line in log file: %s", err)
		}
		assert.Regexp(t, `[0-9]{4}\-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} \[ERROR\] Test timestamp\n`, actual)
	})
}
