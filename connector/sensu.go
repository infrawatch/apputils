package connector

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
	"github.com/streadway/amqp"
)

const (
	//QueueNameKeepAlives is the name of queue used by Sensu server for receiving keepalive messages
	QueueNameKeepAlives = "keepalives"
	//QueueNameResults is the name of queue used by Sensu server for receiving check result messages
	QueueNameResults     = "results"
	defaultClientAddress = "127.0.0.1"
	defaultInterval      = 30
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
	logger            *logging.Logger
	queueName         string
	exchangeName      string
	inConnection      *amqp.Connection
	outConnection     *amqp.Connection
	inChannel         *amqp.Channel
	outChannel        *amqp.Channel
	queue             amqp.Queue
	consumer          <-chan amqp.Delivery
}

//CreateSensuConnector creates the connector and connects on given RabbitMQ service with Sensu server on appropriate channels
func CreateSensuConnector(logger *logging.Logger, address string, clientName string, clientAddress string, keepaliveInterval int64, subscriptions []string) (*SensuConnector, error) {
	connector := SensuConnector{
		Address:           address,
		Subscription:      subscriptions,
		ClientName:        clientName,
		exchangeName:      fmt.Sprintf("client:%s", clientName),
		queueName:         fmt.Sprintf("%s-infrawatch-%d", clientName, time.Now().Unix()),
		ClientAddress:     clientAddress,
		KeepaliveInterval: keepaliveInterval,
		logger:            logger,
	}

	if err := connector.Connect(); err != nil {
		return &connector, fmt.Errorf("Error while connecting to RabbitMQ")
	}

	return &connector, nil
}

//ConnectSensu creates new Sensu connector from the given configuration file
func ConnectSensu(cfg config.Config, logger *logging.Logger) (*SensuConnector, error) {
	var err error
	var opt *config.Option

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("sensu/connection")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Sensu.Connection.Address")
	default:
		return nil, fmt.Errorf("Unknown Config type")
	}
	if err != nil {
		return nil, err
	}
	if opt == nil {
		return nil, fmt.Errorf("Failed to get connection URL from configuration file")
	}
	addr := opt.GetString()

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("sensu/subscriptions")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Sensu.Connection.Subscriptions")
	}
	if err != nil {
		return nil, err
	}
	subs := []string{"all"}
	if opt != nil {
		subs = opt.GetStrings(",")
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("sensu/client_name")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Sensu.Client.Name")
	}
	if err != nil {
		return nil, err
	}
	clientName := defaultClientName
	if opt != nil {
		clientName = opt.GetString()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("sensu/client_address")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Sensu.Client.Address")
	}
	if err != nil {
		return nil, err
	}
	clientAddr := defaultClientAddress
	if opt != nil {
		clientAddr = opt.GetString()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("sensu/keepalive_interval")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Sensu.Connection.KeepaliveInterval")
	}
	if err != nil {
		return nil, err
	}
	interval := int64(defaultInterval)
	if opt != nil {
		interval = opt.GetInt()
	}

	return CreateSensuConnector(logger, addr, clientName, clientAddr, interval, subs)
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
	failed := []string{}
	for _, sub := range conn.Subscription {
		err := conn.inChannel.QueueBind(
			conn.queue.Name, // queue name
			"",              // routing key
			sub,             // exchange
			false,
			nil,
		)
		if err != nil {
			failed = append(failed, err.Error())
			conn.logger.Metadata(logging.Metadata{"subscription": sub, "error": err})
			conn.logger.Warn("Failed to subscribe.")
		}
	}
	if len(failed) == len(conn.Subscription) {
		return fmt.Errorf("Failed to subscribe to all channels: %s", strings.Join(failed, "; "))
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
	//TODO(mmagr): implement stopping goroutines on Disconnect
	// receiving loop
	go func() {
		for req := range conn.consumer {
			var request CheckRequest
			err := json.Unmarshal(req.Body, &request)
			req.Ack(false)
			if err == nil {
				outchan <- request
			} else {
				conn.logger.Metadata(logging.Metadata{"error": err, "request-body": req.Body})
				conn.logger.Warn("Failed to unmarshal request body.")
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
					conn.logger.Metadata(logging.Metadata{"error": err})
					conn.logger.Error("Failed to marshal execution result.")
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
					conn.logger.Metadata(logging.Metadata{"error": err})
					conn.logger.Error("Failed to publish execution result.")
				}
			default:
				conn.logger.Metadata(logging.Metadata{"type": fmt.Sprintf("%T", res)})
				conn.logger.Debug("Received execution result with invalid type.")
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
				conn.logger.Metadata(logging.Metadata{"error": err})
				conn.logger.Error("Failed to marshal keepalive body.")
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
				conn.logger.Metadata(logging.Metadata{"error": err})
				conn.logger.Error("Failed to publish keepalive body.")
			}
			time.Sleep(time.Duration(conn.KeepaliveInterval) * time.Second)
		}
	}()
}
