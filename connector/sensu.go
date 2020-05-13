package connector

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
	"github.com/streadway/amqp"
)

const (
	//QueueNameKeepAlives is the name of queue used by Sensu server for receiving keepalive messages
	QueueNameKeepAlives = "keepalives"
	//QueueNameResults is the name of queue used by Sensu server for receiving check result messages
	QueueNameResults = "results"
)

//Result contains data about check execution
type Result struct {
	Command  string  `json:"command"`
	Name     string  `json:"name"`
	Issued   int64   `json:"issued"`
	Executed int64   `json:"executed"`
	Duration float64 `json:"duration"`
	Output   string  `json:"output"`
	Status   int     `json:"status"`
}

//CheckResult represents message structure for sending check results back to Sensu server
type CheckResult struct {
	Client string `json:"client"`
	Result Result `json:"check"`
}

//CheckRequest is the output of the connector's listening loop
type CheckRequest struct {
	Command string `json:"command"`
	Name    string `json:"name"`
	Issued  int64  `json:"issued"`
}

//Keepalive holds structure for Sensu KeepAlive messages
type Keepalive struct {
	Name         string   `json:"name"`
	Address      string   `json:"address"`
	Subscription []string `json:"subscriptions"`
	Version      string   `json:"version"`
	Timestamp    int64    `json:"timestamp"`
}

//SensuConnector holds all data and functions required for communication with Sensu (1.x) server via RabbitMQ
type SensuConnector struct {
	Address           string
	Subscription      []string
	ClientName        string
	ClientAddress     string
	KeepaliveInterval int64
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
func NewConnector(cfg config.Config, logger *logging.Logger) (*SensuConnector, error) {
	connector := SensuConnector{}
	switch conf := cfg.(type) {
	case config.INIConfig:
		if addr, err := conf.GetOption("sensu/connection"); err != nil {
			connector.Address = addr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get connection URL from configuration file at sensu/connection")
		}
		if subs, err := conf.GetOption("sensu/subscriptions"); err != nil {
			connector.Subscription = subs.GetStrings(",")
		} else {
			return &connector, fmt.Errorf("Failed to get subscription channels from configuration file at sensu/subscriptions")
		}
		if clientName, err := conf.GetOption("sensu/client_name"); err != nil {
			connector.ClientName = clientName.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client name from configuration file at sensu/client_name")
		}
		if clientAddr, err := conf.GetOption("sensu/client_address"); err != nil {
			connector.ClientAddress = clientAddr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client address from configuration file at sensu/client_address")
		}
		if interval, err := conf.GetOption("sensu/keepalive_interval"); err != nil {
			connector.KeepaliveInterval = interval.GetInt()
		} else {
			return &connector, fmt.Errorf("Failed to get keepalive interval from configuration file at sensu/keepalive_interval")
		}
	case config.JSONConfig:
		if addr, err := conf.GetOption("Sensu.Connection.Address"); err != nil {
			connector.Address = addr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get connection URL from configuration file at Sensu.Connection.Address")
		}
		if subs, err := conf.GetOption("Sensu.Connection.Subscriptions"); err != nil {
			connector.Subscription = subs.GetStrings(",")
		} else {
			return &connector, fmt.Errorf("Failed to get subscription channels from configuration file at Sensu.Connection.Subscriptions")
		}
		if interval, err := conf.GetOption("Sensu.Connection.KeepaliveInterval"); err != nil {
			connector.KeepaliveInterval = interval.GetInt()
		} else {
			return &connector, fmt.Errorf("Failed to get keepalive interval from configuration file at Sensu.Connection.KeepaliveInterval")
		}
		if clientName, err := conf.GetOption("Sensu.Client.Name"); err != nil {
			connector.ClientName = clientName.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client name from configuration file at Sensu.Client.Name")
		}
		if clientAddr, err := conf.GetOption("Sensu.Client.Address"); err != nil {
			connector.ClientAddress = clientAddr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client address from configuration file at Sensu.Client.Address")
		}
	}

	connector.log = logger
	connector.exchangeName = fmt.Sprintf("client:%s", connector.ClientName)
	connector.queueName = fmt.Sprintf("%s-infrawatch-%d", connector.ClientName, time.Now().Unix())

	err := connector.Connect()
	if err != nil {
		return nil, err
	}
	return &connector, nil
}

//Connect connects to RabbitMQ server and
func (conn *SensuConnector) Connect() error {
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

//Reconnect tries to reconnect connector to RabbitMQ
func (conn *SensuConnector) Reconnect() error {

	return nil
}

//Disconnect closes all connections
func (conn *SensuConnector) Disconnect() {
	conn.inChannel.Close()
	conn.outChannel.Close()
	conn.inConnection.Close()
	conn.outConnection.Close()
}

//Start starts all processing loops. Channel outchan will contain received CheckRequest messages from Sensu server
// and through inchan CheckResult messages are sent back to Sensu server
func (conn *SensuConnector) Start(outchan chan interface{}, inchan chan interface{}) {
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
			case CheckResult:
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
				conn.log.Metadata(map[string]interface{}{"type": fmt.Sprintf("%T", res)})
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
