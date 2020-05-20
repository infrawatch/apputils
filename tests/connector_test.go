package tests

import (
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/connector"
	"github.com/infrawatch/apputils/logging"
	"github.com/stretchr/testify/assert"
)

const (
	QDRMsg        = "{\"message\": \"smart gateway test\"}"
	ConfigContent = `{
	"Amqp1": {
		"Connection": {
			"Address": "amqp://127.0.0.1:5672/collectd/telemetry",
		  "SendTimeout": 2
		},
		"Client": {
			"Name": "connectortest"
		}
	},
	"Loki": {
		"Connection": {
			"Address": "http://localhost:3100",
			"BatchSize": 4,
			"MaxWaitTime": 50
		}
	}
}
`
)

type MockedConnection struct {
	Address     string
	SendTimeout int
}

type MockedClient struct {
	Name string
}

type MockedLokiConnection struct {
	Address     string
	BatchSize   int
	MaxWaitTime int
}

func TestAMQP10SendAndReceiveMessage(t *testing.T) {
	tmpdir, err := ioutil.TempDir(".", "connector_test_tmp")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	logpath := path.Join(tmpdir, "test.log")
	logger, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		t.Fatalf("Failed to open log file %s. %s\n", logpath, err)
	}
	defer logger.Destroy()

	metadata := map[string][]config.Parameter{
		"Amqp1": []config.Parameter{
			config.Parameter{Name: "LogFile", Tag: ``, Default: logpath, Validators: []config.Validator{}},
		},
	}
	cfg := config.NewJSONConfig(metadata, logger)
	cfg.AddStructured("Amqp1", "Client", ``, MockedClient{})
	cfg.AddStructured("Amqp1", "Connection", ``, MockedConnection{})

	err = cfg.ParseBytes([]byte(ConfigContent))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	conn, err := connector.NewAMQP10Connector(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to connect to QDR: %s", err)
	}
	conn.Connect()
	err = conn.CreateReceiver("qdrtest", -1)
	if err != nil {
		t.Fatalf("Failed to create receiver: %s", err)
	}

	receiver := make(chan interface{})
	sender := make(chan interface{})
	conn.Start(receiver, sender)

	t.Run("Test receive", func(t *testing.T) {
		t.Parallel()
		data := <-receiver
		assert.Equal(t, QDRMsg, (data.(connector.AMQP10Message)).Body)
	})
	t.Run("Test send and ACK", func(t *testing.T) {
		t.Parallel()
		sender <- connector.AMQP10Message{Address: "qdrtest", Body: QDRMsg}
	})
}

func TestLoki(t *testing.T) {
	tmpdir, err := ioutil.TempDir(".", "connector_test_tmp")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(tmpdir)
	logpath := path.Join(tmpdir, "test.log")
	logger, err := logging.NewLogger(logging.DEBUG, logpath)
	if err != nil {
		t.Fatalf("Failed to open log file %s. %s\n", logpath, err)
	}
	defer logger.Destroy()

	metadata := map[string][]config.Parameter{
		"Loki": []config.Parameter{
			config.Parameter{Name: "LogFile", Tag: ``, Default: logpath, Validators: []config.Validator{}},
		},
	}
	cfg := config.NewJSONConfig(metadata, logger)
	cfg.AddStructured("Loki", "Connection", ``, MockedLokiConnection{})
	err = cfg.ParseBytes([]byte(ConfigContent))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	bs, err := cfg.GetOption("Loki.Connection.BatchSize")
	if err != nil {
		t.Fatalf("Failed to get batch size from config file: %s", ConfigContent)
	}
	batchSize := int(bs.GetInt())
	testId := strconv.FormatInt(time.Now().UnixNano(), 16)

	client, err := connector.NewLokiConnector(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create loki client: %s", err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatalf("Failed to connect to loki: %s", err)
	}

	defer func() {
		client.Disconnect()
	}()

	t.Run("Test sending whole streams", func(t *testing.T) {
		c, err := connector.NewLokiConnector(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create loki client: %s", err)
		}
		err = c.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to loki: %s", err)
		}
		receiver := make(chan interface{})
		sender := make(chan interface{})
		c.Start(receiver, sender)
		defer func() {
			c.Disconnect()
		}()

		currentTime := time.Duration(time.Now().UnixNano())
		var messages []connector.Message
		for i := 0; i < batchSize; i++ {
			labels := make(map[string]string)
			labels["test"] = "streams"
			labels["unique"] = testId
			labels["order"] = strconv.FormatInt(int64(i), 10)
			message1 := connector.Message{
				Time:    currentTime,
				Message: "test message streams1",
			}
			message2 := connector.Message{
				Time:    currentTime,
				Message: "test message streams2",
			}
			messages = []connector.Message{message1, message2}
			sender <-c.CreateStream(labels, messages)
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"streams\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, 0, batchSize * 5)
		if err != nil {
			t.Fatalf("Couldn't query loki after push to test streams: %s", err)
		}
		assert.Equal(t, batchSize * 2, len(answer), "Query after streams test returned wrong count of results")
		for _, message := range messages {
			assert.Contains(t, answer, message, "Wrong test message when querying for streams test results")
		}
	})

	t.Run("Test sending single logs", func(t *testing.T) {
		c, err := connector.NewLokiConnector(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create loki client: %s", err)
		}
		err = c.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to loki: %s", err)
		}
		receiver := make(chan interface{})
		sender := make(chan interface{})
		c.Start(receiver, sender)
		defer func() {
			c.Disconnect()
		}()

		currentTime := time.Duration(time.Now().UnixNano())
		for i := 0; i < batchSize; i++ {
			labels := make(map[string]string)
			labels["test"] = "singleLog"
			labels["unique"] = testId
			labels["order"] = strconv.FormatInt(int64(i), 10)
			message := connector.LokiLog {
				LogMessage: "Test message single logs",
				Timestamp: currentTime,
				Labels: labels,
			}
			sender <- message
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"singleLog\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, 0, batchSize)
		if err != nil {
			t.Fatalf("Couldn't query loki after push to test single logs: %s", err)
		}
		assert.Equal(t, batchSize, len(answer), "Query after single log test returned wrong count of results")
		for _, message := range answer {
			assert.Equal(t, "Test message single logs", message.Message, "Wrong test message when querying for single log test results")
			assert.Equal(t, currentTime, message.Time, "Wrong timestamp in queried single log test message")
		}
	})

	// push a whole batch
	t.Run("Test sending in batches", func(t *testing.T) {
		c, err := connector.NewLokiConnector(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create loki client: %s", err)
		}
		err = c.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to loki: %s", err)
		}
		receiver := make(chan interface{})
		sender := make(chan interface{})
		c.Start(receiver, sender)
		defer func() {
			c.Disconnect()
		}()

		currentTime := time.Duration(time.Now().UnixNano())
		var messages []connector.Message
		for i := 0; i < batchSize; i++ {
			labels := make(map[string]string)
			labels["test"] = "batch"
			labels["unique"] = testId
			labels["order"] = strconv.FormatInt(int64(i), 10)
			message := connector.Message{
				Time:    currentTime,
				Message: "test message batch",
			}
			messages = []connector.Message{message}
			sender <-c.CreateStream(labels, messages)
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"batch\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, 0, batchSize)
		if err != nil {
			t.Fatalf("Couldn't query loki after batch push: %s", err)
		}
		assert.Equal(t, batchSize, len(answer), "Query after batch test returned wrong count of results")
		for _, message := range messages {
			assert.Contains(t, answer, message, "Wrong test message when querying for batch test results")
		}
	})

	// push just one message and wait for the maxWaitTime to pass
	t.Run("Test waiting for maxWaitTime to pass", func(t *testing.T) {
		c, err := connector.NewLokiConnector(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create loki client: %s", err)
		}
		err = c.Connect()
		if err != nil {
			t.Fatalf("Failed to connect to loki: %s", err)
		}
		receiver := make(chan interface{})
		sender := make(chan interface{})
		c.Start(receiver, sender)
		defer func() {
			c.Disconnect()
		}()

		labels := make(map[string]string)
		labels["test"] = "single"
		labels["unique"] = testId
		currentTime := time.Duration(time.Now().UnixNano())
		message := connector.Message{
			Time:    currentTime,
			Message: "test message single",
		}
		messages := []connector.Message{message}
		sender <- c.CreateStream(labels, messages)
		time.Sleep(80 * time.Millisecond)

		// query it back
		queryString := "{test=\"single\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, 0, batchSize)
		if err != nil {
			t.Fatalf("Couldn't query loki after testing maxWaitTime: %s", err)
		}
		assert.Equal(t, 1, len(answer), "Query after maxWaitTime test returned wrong count of results")
		for _, message := range answer {
			assert.Equal(t, messages[0], message, "Wrong test message when querying for maxWaitTime test results")
		}
	})
}
