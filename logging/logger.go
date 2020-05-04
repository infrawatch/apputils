package logging

import (
	"bytes"
	"fmt"
	"os"
	"time"
)

type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

func (self LogLevel) String() string {
	return [...]string{"DEBUG", "INFO", "WARN", "ERROR"}[self]
}

type Logger struct {
	Level     LogLevel
	Timestamp bool
	metadata  map[string]interface{}
	logfile   *os.File
}

func NewLogger(level LogLevel, path string) (*Logger, error) {
	var logger Logger
	logger.Level = level
	logger.Timestamp = false
	logger.metadata = make(map[string]interface{})

	logfile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	logger.logfile = logfile

	return &logger, nil
}

func (self *Logger) Destroy() error {
	return self.logfile.Close()
}

func (self *Logger) Metadata(metadata map[string]interface{}) {
	self.metadata = metadata
}

func(self *Logger) SetLogLevel(level LogLevel) {
	self.Level = level
}

func(self *Logger) SetFile(path string, permissions os.FileMode) error {
	newLogfile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, permissions)
	if err != nil {
		self.Warn("Couldn't open new log file, leaving the old one")
		return err
	}
	self.logfile.Close()
	self.logfile = newLogfile
	return nil
}

func (self *Logger) formatMetadata() (string, error) {
	//var build strings.Builder
	// Note: we need to support go-1.9.2 because of CentOS7
	var build bytes.Buffer
	if len(self.metadata) > 0 {
		joiner := ""
		for key, item := range self.metadata {
			_, err := fmt.Fprintf(&build, "%s%s: %v", joiner, key, item)
			if err != nil {
				return build.String(), err
			}
			if len(joiner) == 0 {
				joiner = ", "
			}
		}
	}
	// clear metadata for next use
	self.metadata = make(map[string]interface{})
	return build.String(), nil
}

func (self *Logger) writeRecord(level LogLevel, message string) error {
	metadata, err := self.formatMetadata()
	if err != nil {
		return err
	}

	//var build strings.Builder
	// Note: we need to support go-1.9.2 because of CentOS7
	var build bytes.Buffer
	if self.Timestamp {
		_, err = build.WriteString(time.Now().Format("2006-01-02 15:04:05 "))
	}

	_, err = build.WriteString(fmt.Sprintf("[%s] ", level))
	if err != nil {
		return nil
	}
	_, err = build.WriteString(message)
	if err != nil {
		return nil
	}
	if len(metadata) > 0 {
		_, err = build.WriteString(fmt.Sprintf(" [%s]", metadata))
		if err != nil {
			return nil
		}
	}
	_, err = build.WriteString("\n")
	if err != nil {
		return nil
	}
	_, err = self.logfile.WriteString(build.String())
	return err
}

func (self *Logger) Debug(message string) error {
	if self.Level == DEBUG {
		return self.writeRecord(DEBUG, message)
	}
	return nil
}

func (self *Logger) Info(message string) error {
	if self.Level <= INFO {
		return self.writeRecord(INFO, message)
	}
	return nil
}

func (self *Logger) Warn(message string) error {
	if self.Level <= WARN {
		return self.writeRecord(WARN, message)
	}
	return nil
}

func (self *Logger) Error(message string) error {
	if self.Level <= ERROR {
		return self.writeRecord(ERROR, message)
	}
	return nil
}
