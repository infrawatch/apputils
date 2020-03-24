package tests

import (
    "testing"
    "fmt"
    "time"
    "strings"
    "strconv"

    "github.com/stretchr/testify/assert"
    "github.com/infrawatch/apputils/connector"
)

const QDRURL = "amqp://127.0.0.1:5672/collectd/telemetry"
const QDRMsg = "{\"message\": \"smart gateway test\"}"

func TestAMQP10SendAndReceiveMessage(t *testing.T) {
	sender := connector.NewAMQPSender(QDRURL, true)
	receiver := connector.NewAMQPServer(QDRURL, true, 1, 0, "metrics-test")
	ackChan := sender.GetAckChannel()
	t.Run("Test receive", func(t *testing.T) {
		t.Parallel()
		data := <-receiver.GetNotifier()
		assert.Equal(t, QDRMsg, data)
		fmt.Printf("Finished send")
	})
	t.Run("Test send and ACK", func(t *testing.T) {
		t.Parallel()
		sender.Send(QDRMsg)
		// otherwise receiver blocks
		assert.Equal(t, 1, <-receiver.GetStatus())
		assert.Equal(t, true, <-receiver.GetDoneChan())
		outcome := <-ackChan
		assert.Equal(t, "smart-gateway-ack", outcome.Value.(string))
	})
}

func TestLoki(t *testing.T) {
    // TODO: read these from config
    server := "http://localhost"
    port := "3100"
    batchSize := 4
    maxWaitTime := 50 * time.Millisecond
    testId := strconv.FormatInt(time.Now().UnixNano(), 16)
    url := strings.Join([]string{server, port}, ":")

    client, err := connector.CreateClient(url, batchSize, maxWaitTime)
    if err != nil {
        t.Fatalf("Failed to create loki client: %s", err)
    }
    assert.Equal(t, client.IsReady(), true, "The client isn't ready")

    defer func() {
        client.Shutdown()
    }()


    // push a whole batch
    t.Run("Test sending in batches", func(t *testing.T) {
        c, err := connector.CreateClient(url, batchSize, 10 * time.Second)
        if err != nil {
            t.Fatalf("Failed to create loki client: %s", err)
        }
        defer func() {
            c.Shutdown()
        }()

        currentTime := time.Duration(time.Now().UnixNano())
        for i := 0; i < batchSize; i++ {
            labels := make(map[string]string)
            labels["test"] = "batch"
            labels["unique"] = testId
            labels["order"] = strconv.FormatInt(int64(i), 10)
            message := connector.Message {
                Time: currentTime,
                Message: "test message batch",
            }
            messages := []connector.Message{message}
            c.AddStream(labels, messages)
        }
        time.Sleep(10 * time.Millisecond)

        // query it back
        queryString := "{test=\"batch\",unique=\"" + testId + "\"}"
        answer, err := c.Query(queryString)
        if err != nil {
            t.Fatalf("Couldn't query loki after batch push: %s", err)
        }
        assert.Equal(t, batchSize, len(answer), "Query after batch test returned wrong count of results")
        for _, message := range answer {
            assert.Equal(t, "test message batch", message.Message, "Wrong test message when querying for batch test results")
            assert.Equal(t, currentTime, message.Time, "Wrong timestamp in queried batch test message")
        }
    })


    // push just one message and wait for the maxWaitTime to pass
    t.Run("Test waiting for maxWaitTime to pass", func(t *testing.T) {
        c, err := connector.CreateClient(url, batchSize, maxWaitTime)
        if err != nil {
            t.Fatalf("Failed to create loki client: %s", err)
        }
        defer func() {
            c.Shutdown()
        }()

        labels := make(map[string]string)
        labels["test"] = "single"
        labels["unique"] = testId
        currentTime := time.Duration(time.Now().UnixNano())
        message := connector.Message {
            Time: currentTime,
            Message: "test message single",
        }
        messages := []connector.Message{message}
        c.AddStream(labels, messages)
        time.Sleep(80 * time.Millisecond)

        // query it back
        queryString := "{test=\"single\",unique=\"" + testId + "\"}"
        answer, err := c.Query(queryString)
        if err != nil {
            t.Fatalf("Couldn't query loki after testing maxWaitTime: %s", err)
        }
        assert.Equal(t, 1, len(answer), "Query after maxWaitTime test returned wrong count of results")
        for _, message := range answer {
            assert.Equal(t, "test message single", message.Message, "Wrong test message when querying for maxWaitTime test results")
            assert.Equal(t, currentTime, message.Time, "Wrong timestamp in queried maxWaitTime test message")
        }
    })


    // test sending multiple messages in a single stream
    t.Run("Test sending multiple messages in a single stream", func(t *testing.T) {
        c, err := connector.CreateClient(url, batchSize, maxWaitTime)
        if err != nil {
            t.Fatalf("Failed to create loki client: %s", err)
        }
        defer func() {
            c.Shutdown()
        }()

        labels := make(map[string]string)
        labels["test"] = "multiple_in_a_stream"
        labels["unique"] = testId
        currentTime := time.Duration(time.Now().UnixNano())
        var messages []connector.Message
        for i := 0; i < 2; i++ {
            message := connector.Message {
                Time: currentTime,
                Message: strconv.FormatInt(int64(i), 10),
            }
            messages = append(messages, message)
        }
        c.AddStream(labels, messages)
        time.Sleep(80 * time.Millisecond)

        // query it back
        queryString := "{test=\"multiple_in_a_stream\",unique=\"" + testId + "\"}"
        answer, err := c.Query(queryString)
        if err != nil {
            t.Fatalf("Couldn't query loki after pushing multiple messages in a stream: %s", err)
        }
        assert.Equal(t, 2, len(answer), "Query after sending multiple messages in a single stream returned wrong count of results")
        // we should get one message, that equals "0" and one
        // message, that equals "1", but we don't know in
        // which order
        if ((answer[0].Message != "0" || answer[1].Message != "1") &&
            (answer[0].Message != "1" || answer[1].Message != "0")) {
            t.Fatalf("Wrong test message when querying for \"send multiple messages in a single stream\" results")
            assert.Equal(t, currentTime, answer[0].Time, "Wrong timestamp in queried \"send multiple messages in a single stream\"test message")
            assert.Equal(t, currentTime, answer[1].Time, "Wrong timestamp in queried \"send multiple messages in a single stream\"test message")
        }
    })
}

