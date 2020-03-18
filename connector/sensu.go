package sensu

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
	"github.com/streadway/amqp"
)

const (
	QueueNameKeepAlives = "keepalives"
	QueueNameResults    = "results"
)

//CheckRequest is the output of the connector's listening loop
type CheckRequest struct {
	Command string
	Name    string
	Issued  int64
}

//Keepalive holds structure for Sensu KeepAlive messages
type Keepalive struct {
	Name         string   `json:"name"`
	Address      string   `json:"address"`
	Subscription []string `json:"subscriptions"`
	Version      string   `json:"version"`
	Timestamp    int64    `json:"timestamp"`
}

//Connector holds all data and functions required for communication with Sensu (1.x) server via RabbitMQ
type Connector struct {
	Address           string
	Subscription      []string
	ClientName        string
	ClientAddress     string
	KeepaliveInterval int
	log               *logging.Logger
	queueName         string
	exchangeName      string
	inConnection      *amqp.Connection
	outConnection     *amqp.Connection
	inChannel         *amqp.Channel
	outChannel        *amqp.Channel
	queue             amqp.Queue
	consumer          <-chan amqp.Delivery
}

//NewConnector creates new Sensu connector from the given configuration file
func NewConnector(cfg *config.Config, logger *logging.Logger) (*Connector, error) {
	var connector Connector
	connector.Address = cfg.Sections["sensu"].Options["connection"].GetString()
	connector.Subscription = cfg.Sections["sensu"].Options["subscriptions"].GetStrings(",")
	connector.ClientName = cfg.Sections["sensu"].Options["client_name"].GetString()
	connector.ClientAddress = cfg.Sections["sensu"].Options["client_address"].GetString()
	connector.KeepaliveInterval = cfg.Sections["sensu"].Options["keepalive_interval"].GetInt()

	connector.log = logger
	connector.exchangeName = fmt.Sprintf("client:%s", connector.ClientName)
	connector.queueName = fmt.Sprintf("%s-collectd-%d", connector.ClientName, time.Now().Unix())

	err := connector.Connect()
	if err != nil {
		return nil, err
	}
	return &connector, nil
}

//Connect connects to RabbitMQ server and
func (conn *Connector) Connect() error {
	var err error
	conn.inConnection, err = amqp.Dial(conn.Address)
	if err != nil {
		return err
	}

	conn.outConnection, err = amqp.Dial(conn.Address)
	if err != nil {
		return err
	}

	conn.inChannel, err = conn.inConnection.Channel()
	if err != nil {
		return err
	}

	conn.outChannel, err = conn.outConnection.Channel()
	if err != nil {
		return err
	}

	// declare an exchange for this client
	err = conn.inChannel.ExchangeDeclare(
		conn.exchangeName, // name
		"fanout",          // type
		false,             // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		return err
	}

	// declare a queue for this client
	conn.queue, err = conn.inChannel.QueueDeclare(
		conn.queueName, // name
		false,          // durable
		false,          // delete unused
		false,          // exclusive
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		return err
	}

	// register consumer
	conn.consumer, err = conn.inChannel.Consume(
		conn.queue.Name, // queue
		conn.ClientName, // consumer
		false,           // auto ack
		false,           // exclusive
		false,           // no local
		false,           // no wait
		nil,             // args
	)
	if err != nil {
		return err
	}

	// bind client queue with subscriptions
	for _, sub := range conn.Subscription {
		err := conn.inChannel.QueueBind(
			conn.queue.Name, // queue name
			"",              // routing key
			sub,             // exchange
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (conn *Connector) ReConnect() error {

	return nil
}

func (conn *Connector) Disconnect() {
	conn.inChannel.Close()
	conn.outChannel.Close()
	conn.inConnection.Close()
	conn.outConnection.Close()
}

func (conn *Connector) Start(outchan chan interface{}, inchan chan interface{}) {
	// receiving loop
	go func() {
		for req := range conn.consumer {
			var request CheckRequest
			err := json.Unmarshal(req.Body, &request)
			req.Ack(false)
			if err == nil {
				outchan <- request
			} else {
				conn.log.Metadata(map[string]interface{}{"error": err, "request-body": req.Body})
				conn.log.Warn("Failed to unmarshal request body.")
			}
		}
	}()

	// sending loop
	go func() {
		for res := range inchan {
			switch result := res.(type) {
			case Result:
				body, err := json.Marshal(result)
				if err != nil {
					conn.log.Metadata(map[string]interface{}{"error": err})
					conn.log.Error("Failed to marshal execution result.")
					continue
				}
				err = conn.outChannel.Publish(
					"",               // exchange
					QueueNameResults, // queue
					false,            // mandatory
					false,            // immediate
					amqp.Publishing{
						Headers:         amqp.Table{},
						ContentType:     "text/json",
						ContentEncoding: "",
						Body:            body,
						DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
						Priority:        0,              // 0-9
					})
				if err != nil {
					conn.log.Metadata(map[string]interface{}{"error": err})
					conn.log.Error("Failed to publish execution result.")
				}
			default:
				conn.log.Metadata(map[string]interface{}{"type": fmt.Sprintf("%t", res)})
				conn.log.Error("Received execution result with invalid type.")
			}
		}
	}()

	// keepalive loop
	go func() {
		for {
			body, err := json.Marshal(Keepalive{
				Name:         conn.ClientName,
				Address:      conn.ClientAddress,
				Subscription: conn.Subscription,
				Version:      "collectd",
				Timestamp:    time.Now().Unix(),
			})
			if err != nil {
				conn.log.Metadata(map[string]interface{}{"error": err})
				conn.log.Error("Failed to marshal keepalive body.")
				continue
			}
			err = conn.outChannel.Publish(
				"",                  // exchange
				QueueNameKeepAlives, // queue
				false,               // mandatory
				false,               // immediate
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "text/json",
					ContentEncoding: "",
					Body:            body,
					DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
					Priority:        0,              // 0-9
				})
			if err != nil {
				conn.log.Metadata(map[string]interface{}{"error": err})
				conn.log.Error("Failed to publish keepalive body.")
			}
			time.Sleep(time.Duration(conn.KeepaliveInterval) * time.Second)
		}
	}()
}
