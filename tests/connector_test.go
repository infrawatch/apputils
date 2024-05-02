package tests

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/connector/amqp10"
	"github.com/infrawatch/apputils/connector/loki"
	sensuPackage "github.com/infrawatch/apputils/connector/sensu"
	"github.com/infrawatch/apputils/connector/unixSocket"
	"github.com/infrawatch/apputils/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	QDRMsg1 = "{\"labels\":{\"check\":\"test\",\"client\":\"fedora\",\"severity\":\"OKAY\"},\"annotations\":{\"command\":\"echo 'Test transfer'\",\"duration\":0.002853846,\"executed\":1675108402," +
		"\"issued\":1675108402,\"output\":\"Test transfer\\n\",\"status\":0,\"ves\":\"{\\\"commonEventHeader\\\":{\\\"domain\\\":\\\"heartbeat\\\",\\\"eventType\\\":\\\"checkResult\\\"," +
		"\\\"eventId\\\":\\\"fedora-test\\\",\\\"priority\\\":\\\"Normal\\\",\\\"reportingEntityId\\\":\\\"c1d13353-82aa-4370-bc53-db0d60d79c12\\\",\\\"reportingEntityName\\\":\\\"fedora\\\"," +
		"\\\"sourceId\\\":\\\"c1d13353-82aa-4370-bc53-db0d60d79c12\\\",\\\"sourceName\\\":\\\"fedora-collectd-sensubility\\\",\\\"startingEpochMicrosec\\\":1675108402,\\\"lastEpochMicrosec\\\":1675108402}," +
		"\\\"heartbeatFields\\\":{\\\"additionalFields\\\":{\\\"check\\\":\\\"test\\\",\\\"command\\\":\\\"echo 'Test transfer'\\\",\\\"duration\\\":\\\"0.002854\\\",\\\"executed\":\\\"1675108402\"," +
		"\\\"issued\\\":\\\"1675108402\\\",\\\"output\\\":\"Test transfer\\n\\\",\\\"status\\\":\\\"0\\\"}}}\"},\"startsAt\":\"2023-01-30T20:53:22+01:00\"}}"
	QDRMsg2       = "{\"message\": \"smart gateway reconnect test\"}"
	ConfigContent = `{
	"LogLevel": "Debug",
	"Amqp1": {
		"Connection": {
			"Address": "amqp://127.0.0.1:5666",
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
			"MaxWaitTime": 50,
			"TenantID": "tenant1"
		}
	},
	"Socket": {
		"In": {
			"Address": "/tmp/socktest"
		},
		"Out": {
			"Address": "/tmp/socktest"
		}
	}
}
`
	ConfigContent2 = `{
	"Socket": {
		"In": {
			"Address": "/tmp/socktest1"
		}
	}
}
`
	ConfigContent3 = `{
	"Socket": {
		"Out": {
			"Address": "/tmp/socktest1"
		}
	}
}
`
)

type MockedSocket struct {
	Address string
}

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
	TenantID    string
}

func TestUnixSocketSendAndReceiveMessage(t *testing.T) {
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
		"Socket": {},
	}
	cfg := config.NewJSONConfig(metadata, logger)
	cfg.AddStructured("Socket", "In", ``, MockedSocket{})
	cfg.AddStructured("Socket", "Out", ``, MockedSocket{})

	// bidirectional
	err = cfg.ParseBytes([]byte(ConfigContent))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	socket, err := unixSocket.ConnectUnixSocket(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create the socket connector: %s", err)
	}

	receiver := make(chan interface{})
	sender := make(chan interface{})
	socket.Start(receiver, sender)

	// in
	err = cfg.ParseBytes([]byte(ConfigContent2))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	socketIn, err := unixSocket.ConnectUnixSocket(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create the socket connector: %s", err)
	}

	receiverIn := make(chan interface{})
	senderIn := make(chan interface{})
	socketIn.Start(receiverIn, senderIn)

	// out
	err = cfg.ParseBytes([]byte(ConfigContent3))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	socketOut, err := unixSocket.ConnectUnixSocket(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create the socket connector: %s", err)
	}

	receiverOut := make(chan interface{})
	senderOut := make(chan interface{})
	socketOut.Start(receiverOut, senderOut)

	t.Run("Test socket bidirectional.", func(t *testing.T) {
		t.Parallel()
		sender <- "hi socket, this is socket"
		data := <-receiver
		assert.Equal(t, "hi socket, this is socket", data.(string))
	})
	t.Run("Test socket unidirectional.", func(t *testing.T) {
		t.Parallel()
		senderOut <- "hi socket receiver, this is socket sender"
		data := <-receiverIn
		assert.Equal(t, "hi socket receiver, this is socket sender", data.(string))
	})
}

func TestAMQP10SendAndReceiveMessage(t *testing.T) {
	tmpdir, err := os.MkdirTemp(".", "connector_test_tmp")
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
		"Amqp1": {
			{Name: "LogFile", Tag: ``, Default: logpath, Validators: []config.Validator{}},
		},
	}
	cfg := config.NewJSONConfig(metadata, logger)
	cfg.AddStructured("Amqp1", "Client", ``, MockedClient{})
	cfg.AddStructured("Amqp1", "Connection", ``, MockedConnection{})

	err = cfg.ParseBytes([]byte(ConfigContent))
	if err != nil {
		t.Fatalf("Failed to parse config file: %s", err)
	}

	conn, err := amqp10.ConnectAMQP10("ci", cfg, logger)
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
	cwg := conn.Start(receiver, sender)

	t.Run("Test transport", func(t *testing.T) {
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				data := <-receiver
				assert.Equal(t, QDRMsg1, (data.(amqp10.AMQP10Message)).Body)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			sender <- amqp10.AMQP10Message{Address: "qdrtest", Body: QDRMsg1}
			sender <- amqp10.AMQP10Message{Address: "qdrtest", Body: QDRMsg1}
			sender <- amqp10.AMQP10Message{Address: "qdrtest", Body: QDRMsg1}
		}()

		wg.Wait()
	})

	t.Run("Test reconnect of sender", func(t *testing.T) {
		var wg sync.WaitGroup

		require.NoError(t, conn.Reconnect("out", sender, cwg))

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				data := <-receiver
				assert.Equal(t, QDRMsg2, (data.(amqp10.AMQP10Message)).Body)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				sender <- amqp10.AMQP10Message{Address: "qdrtest", Body: QDRMsg2}
			}
		}()

		wg.Wait()
	})

	t.Run("Test reconnect of receiver", func(t *testing.T) {
		var wg sync.WaitGroup

		require.NoError(t, conn.Reconnect("in", receiver, cwg))

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				data := <-receiver
				assert.Equal(t, QDRMsg2, (data.(amqp10.AMQP10Message)).Body)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 3; i++ {
				sender <- amqp10.AMQP10Message{Address: "qdrtest", Body: QDRMsg2}
			}
		}()

		wg.Wait()
	})

	conn.Disconnect()
	cwg.Wait()
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
		"Loki": {
			{Name: "LogFile", Tag: ``, Default: logpath, Validators: []config.Validator{}},
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

	client, err := loki.ConnectLoki(cfg, logger)
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
		c, err := loki.ConnectLoki(cfg, logger)
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
		var messages []loki.Message
		for i := 0; i < batchSize; i++ {
			labels := make(map[string]string)
			labels["test"] = "streams"
			labels["unique"] = testId
			labels["order"] = strconv.FormatInt(int64(i), 10)
			message1 := loki.Message{
				Time:    currentTime,
				Message: "test message streams1",
			}
			message2 := loki.Message{
				Time:    currentTime,
				Message: "test message streams2",
			}
			messages = []loki.Message{message1, message2}
			sender <- c.CreateStream(labels, messages)
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"streams\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, currentTime-1, batchSize*5)
		if err != nil {
			t.Fatalf("Couldn't query loki after push to test streams: %s", err)
		}
		assert.Equal(t, batchSize*2, len(answer), "Query after streams test returned wrong count of results")
		for _, message := range messages {
			assert.Contains(t, answer, message, "Wrong test message when querying for streams test results")
		}
	})

	t.Run("Test sending single logs", func(t *testing.T) {
		c, err := loki.ConnectLoki(cfg, logger)
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
			message := loki.LokiLog{
				LogMessage: "Test message single logs",
				Timestamp:  currentTime,
				Labels:     labels,
			}
			sender <- message
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"singleLog\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, currentTime-1, batchSize)
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
		c, err := loki.ConnectLoki(cfg, logger)
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
		var messages []loki.Message
		for i := 0; i < batchSize; i++ {
			labels := make(map[string]string)
			labels["test"] = "batch"
			labels["unique"] = testId
			labels["order"] = strconv.FormatInt(int64(i), 10)
			message := loki.Message{
				Time:    currentTime,
				Message: "test message batch",
			}
			messages = []loki.Message{message}
			sender <- c.CreateStream(labels, messages)
		}
		time.Sleep(10 * time.Millisecond)

		// query it back
		queryString := "{test=\"batch\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, currentTime-1, batchSize)
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
		c, err := loki.ConnectLoki(cfg, logger)
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
		message := loki.Message{
			Time:    currentTime,
			Message: "test message single",
		}
		messages := []loki.Message{message}
		sender <- c.CreateStream(labels, messages)
		time.Sleep(80 * time.Millisecond)

		// query it back
		queryString := "{test=\"single\",unique=\"" + testId + "\"}"
		answer, err := c.Query(queryString, currentTime-1, batchSize)
		if err != nil {
			t.Fatalf("Couldn't query loki after testing maxWaitTime: %s", err)
		}
		assert.Equal(t, 1, len(answer), "Query after maxWaitTime test returned wrong count of results")
		for _, message := range answer {
			assert.Equal(t, messages[0], message, "Wrong test message when querying for maxWaitTime test results")
		}
	})
}

func TestSensuCommunication(t *testing.T) {
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

	// check for "ci" subscribers is defined in ci/sensu/check.d/test.json
	sensu, err := sensuPackage.CreateSensuConnector(logger, "amqp://sensu:sensu@127.0.0.1:5672//sensu", "ci-unit", "127.0.0.1", 1, []string{"ci"})
	assert.NoError(t, err)

	err = sensu.Connect()
	require.NoError(t, err)

	t.Run("Test communication with sensu-core server", func(t *testing.T) {
		requests := make(chan interface{})
		results := make(chan interface{})
		sensu.Start(requests, results)

		// wait for request from sensu-core server
		for req := range requests {
			switch reqst := req.(type) {
			case sensuPackage.CheckRequest:
				// verify we received awaited check request
				assert.Equal(t, "echo", reqst.Name)
				assert.Equal(t, "echo \"wubba lubba\" && exit 1", reqst.Command)

				// mock result and send it
				result := sensuPackage.CheckResult{
					Client: sensu.ClientName,
					Result: sensuPackage.Result{
						Command:  reqst.Command,
						Name:     reqst.Name,
						Issued:   reqst.Issued,
						Handlers: reqst.Handlers,
						Handler:  reqst.Handler,
						Executed: time.Now().Unix(),
						Duration: time.Millisecond.Seconds(),
						Output:   "wubba lubba",
						Status:   1,
					},
				}
				results <- result
				goto done
			}
		}
	done:
		// wait for sensu handler to create result receive verification file
		time.Sleep(time.Second)

		resp, err := http.Get("http://127.0.0.1:4567/results")
		assert.NoError(t, err)
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err)

		var resultList []sensuPackage.CheckResult
		err = json.Unmarshal(body, &resultList)
		assert.NoError(t, err)
		found := false
		for _, res := range resultList {
			if res.Client == "ci-unit" {
				if res.Result.Name == "echo" && res.Result.Command == "echo \"wubba lubba\" && exit 1" {
					found = true
					break
				}
			}
		}
		assert.True(t, found)
	})
}
