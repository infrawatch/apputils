package tests

import (
    "testing"
    "fmt"

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



