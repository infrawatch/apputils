package amqp10

import (
	"fmt"
	"strings"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"

	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
)

const (
	defaultSendTimeout    = 2
	defaultListenPrefetch = -1
	defaultClientName     = "localhost"
	defaultLinkLimit      = 5
)

//AMQP10Receiver is tagged electron receiver
type AMQP10Receiver struct {
	Receiver electron.Receiver
	Tags     []string
}

//AMQP10Connector is the object to be used for communication with AMQP-1.0 entity
type AMQP10Connector struct {
	Address          string
	ClientName       string
	SendTimeout      int64
	LinkFailureLimit int64
	appName          string
	inConnection     electron.Connection
	outConnection    electron.Connection
	receivers        []AMQP10Receiver
	logger           *logging.Logger
	interrupt        chan bool
}

//AMQP10Message holds received (or to be sent) messages from (to) AMQP-1.0 entity
type AMQP10Message struct {
	Address string
	Body    string
	Tags    []string
}

//CreateAMQP10Connector creates the connector and connects to given AMQP1.0 service
func CreateAMQP10Connector(logger *logging.Logger, address string, clientName string,
	appName string, sendTimeout int64, linkFailureLimit int64, listenPrefetch int64,
	listenChannels []string) (*AMQP10Connector, error) {
	connector := AMQP10Connector{
		Address:          address,
		ClientName:       clientName,
		SendTimeout:      sendTimeout,
		LinkFailureLimit: linkFailureLimit,
		appName:          appName,
		logger:           logger,
		receivers:        make([]AMQP10Receiver, 0),
		interrupt:        make(chan bool),
	}

	// connect
	if err := connector.Connect(); err != nil {
		return &connector, fmt.Errorf("Error while connecting to AMQP")
	}
	// bind to channels
	for _, channel := range listenChannels {
		if len(channel) < 1 {
			continue
		}
		logger.Metadata(map[string]interface{}{
			"channel":  channel,
			"prefetch": listenPrefetch,
		})
		logger.Debug("Creating AMQP receiver for channel")
		if err := connector.CreateReceiver(channel, int(listenPrefetch)); err != nil {
			return &connector, fmt.Errorf("Failed to create receiver: %s", err)
		}
	}
	return &connector, nil
}

//ConnectAMQP10 creates new AMQP1.0 connector from the given configuration file
func ConnectAMQP10(appName string, cfg config.Config, logger *logging.Logger) (*AMQP10Connector, error) {
	var err error
	var opt *config.Option

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/connection")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.Address")
	default:
		return nil, fmt.Errorf("Unknown Config type")
	}
	if err != nil {
		return nil, err
	}
	addr := opt.GetString()

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/send_timeout")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.SendTimeout")
	}
	sendTimeout := int64(defaultSendTimeout)
	if opt != nil && err == nil {
		sendTimeout = opt.GetInt()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/link_failure_limit")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.LinkFailureLimit")
	}
	linkLimit := int64(defaultLinkLimit)
	if opt != nil && err == nil {
		linkLimit = opt.GetInt()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/client_name")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Client.Name")
	}
	clientName := defaultClientName
	if opt != nil && err == nil {
		clientName = opt.GetString()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/listen_channels")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.ListenChannels")
	}
	listen := []string{}
	if opt != nil && err == nil {
		listen = opt.GetStrings(",")
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/listen_prefetch")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.ListenPrefetch")
	}
	prf := int64(defaultListenPrefetch)
	if opt != nil && err == nil {
		prf = opt.GetInt()
	}

	return CreateAMQP10Connector(logger, addr, clientName, appName, sendTimeout, linkLimit, prf, listen)
}

func dial(address, containerName string) (*electron.Connection, error) {
	url, err := amqp.ParseURL(address)
	if err != nil {
		return nil, fmt.Errorf("Error while parsing AMQP1.0 URL: %s", address)
	}

	container := electron.NewContainer(containerName)
	conn, err := container.Dial("tcp", url.Host)
	if err != nil {
		return nil, fmt.Errorf("AMQP dial TCP error: %s", err.Error())
	}
	return &conn, err
}

func (conn *AMQP10Connector) connect(connType string) error {
	container := fmt.Sprintf("%s-%s-%s-%d", conn.ClientName, conn.appName, connType, time.Now().Unix())
	c, err := dial(conn.Address, container)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"container": container,
		})
		conn.logger.Debug("Failed to create AMQP1.0 connection")
		return err
	}

	switch connType {
	case "in":
		conn.inConnection = *c
	case "out":
		conn.outConnection = *c
	}
	return nil
}

//Connect creates input and output connection container for given appname for configured AMQP1.0 node
func (conn *AMQP10Connector) Connect() error {
	if err := conn.connect("in"); err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		conn.logger.Debug("Failed to create incoming connection")
		return err
	}

	if err := conn.connect("out"); err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		conn.logger.Debug("Failed to create ougoing connection")
		return err
	}
	return nil
}

//CreateReceiver creates electron.Receiver for given address
func (conn *AMQP10Connector) CreateReceiver(address string, prefetch int) error {
	addr := strings.TrimPrefix(address, "/")
	parts := strings.Split(addr, ":")

	opts := []electron.LinkOption{electron.Source(parts[0])}
	if prefetch > 0 {
		conn.logger.Metadata(map[string]interface{}{
			"address":  address,
			"prefetch": prefetch,
		})
		conn.logger.Debug("Setting prefetch for address")
		opts = append(opts, electron.Capacity(prefetch), electron.Prefetch(true))
	}

	if conn.inConnection == nil {
		return fmt.Errorf("Connection to AMQP-1.0 node has to be created first.")
	}
	if rcv, err := conn.inConnection.Receiver(opts...); err == nil {
		conn.receivers = append(conn.receivers, AMQP10Receiver{rcv, parts[1:]})
	} else {
		conn.logger.Metadata(map[string]interface{}{
			"address": address,
			"error":   err,
		})
		conn.logger.Debug("Failed to create receiver for given address")
		return err
	}
	return nil
}

//Reconnect tries to reconnect connector to configured AMQP1.0 node
func (conn *AMQP10Connector) Reconnect(connectionType string) error {
	switch connectionType {
	case "in":
		if err := conn.inConnection.WaitTimeout(time.Second); err != nil {
			conn.logger.Metadata(map[string]interface{}{
				"error": err,
			})
			conn.logger.Warn("Failed to disconnect incoming connection")
		}
	case "out":
		if err := conn.outConnection.WaitTimeout(time.Second); err != nil {
			conn.logger.Metadata(map[string]interface{}{
				"error": err,
			})
			conn.logger.Warn("Failed to disconnect outgoing connection")
		}
	default:
		return fmt.Errorf("Wrong connection type. Should be 'in' or 'out'")
	}

	if err := conn.connect(connectionType); err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"connection": connectionType,
			"error":      err,
		})
		conn.logger.Error("Failed to reconnect")
		return err
	}
	return nil
}

//Disconnect closes all connections
func (conn *AMQP10Connector) Disconnect() {
	close(conn.interrupt)
	time.Sleep(time.Second)
	conn.inConnection.Close(nil)
	conn.outConnection.Close(nil)
	conn.logger.Metadata(map[string]interface{}{
		"incoming": conn.inConnection,
		"outgoing": conn.outConnection,
	})
	conn.logger.Debug("Closed connections")
}

func (conn *AMQP10Connector) processIncomingMessage(msg interface{}, outchan chan interface{}, receiver AMQP10Receiver) {
	message := AMQP10Message{Address: receiver.Receiver.Source(), Tags: receiver.Tags}
	switch typedBody := msg.(type) {
	case amqp.List:
		conn.logger.Debug("Received message is a list, recursevily diving inside it.")
		for _, element := range typedBody {
			conn.processIncomingMessage(element, outchan, receiver)
		}
	case amqp.Binary:
		message.Body = typedBody.String()
		outchan <- message
	case string:
		message.Body = typedBody
		outchan <- message
	default:
		conn.logger.Metadata(map[string]interface{}{
			"message": typedBody,
		})
		conn.logger.Debug("Skipped processing of received AMQP1.0 message with invalid type")
		outchan <- message
	}
}

//Start starts all processing loops. Channel outchan will contain received AMQP10Message from AMQP1.0 node
// and through inchan AMQP10Message are sent to configured AMQP1.0 node
func (conn *AMQP10Connector) Start(outchan chan interface{}, inchan chan interface{}) {
	//create listening goroutine for each receiver
	for _, rcv := range conn.receivers {
		go func(receiver AMQP10Receiver) {
			for {
				if msg, err := receiver.Receiver.Receive(); err == nil {
					msg.Accept()
					conn.processIncomingMessage(msg.Message.Body(), outchan, receiver)
					conn.logger.Debug("Message ACKed")
				} else if err == electron.Closed {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"address":    receiver.Receiver.Source(),
					})
					conn.logger.Warn("Channel closed, closing receiver loop")
					//TODO: send message to (future) reconnect loop, where it Reconnect and Start again
					return
				} else {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"address":    receiver.Receiver.Source(),
						"error":      err,
					})
					conn.logger.Error("Received AMQP1.0 error")
				}
			}
		}(rcv)
	}

	//create sending goroutine
	go func(inchan chan interface{}) {
		ackchan := make(chan electron.Outcome)

		// ACK and error verification goroutine
		go func(ackchan chan electron.Outcome) {
			for {
				ack := <-ackchan
				if ack.Error != nil {
					if ack.Error == electron.Timeout {
						conn.logger.Metadata(logging.Metadata{
							"timeout": conn.SendTimeout,
							"id":      ack.Value,
						})
						conn.logger.Error("Was not able to deliver message on time.")
					} else {
						conn.logger.Metadata(logging.Metadata{
							"id":  ack.Value,
							"err": ack.Error,
						})
						conn.logger.Error("Error delivering message.")
					}
				} else if ack.Status != 2 {
					conn.logger.Metadata(logging.Metadata{
						"id":  ack.Value,
						"ack": ack.Status,
					})
					conn.logger.Warn("Sent message was not ACKed.")
				} else {
					conn.logger.Metadata(logging.Metadata{
						"id": ack.Value,
					})
					conn.logger.Debug("Sent message ACKed.")
				}
			}
		}(ackchan)

		// cache of opened links
		senders := map[string]electron.Sender{}
		timeout := time.Duration(conn.SendTimeout) * time.Second

		linkFail := int64(0)
		counter := int64(0)
		for {
			select {
			case msg := <-inchan:
				switch message := msg.(type) {
				case AMQP10Message:
					var sender electron.Sender
					if s, ok := senders[message.Address]; ok {
						sender = s
					} else {
						opts := []electron.LinkOption{electron.Target(message.Address), electron.AtMostOnce()}
						s, err := conn.outConnection.Sender(opts...)
						if err != nil {
							conn.logger.Metadata(map[string]interface{}{
								"connection": conn.Address,
								"message":    message,
								"error":      err,
							})
							conn.logger.Warn("Failed to create AMQP1.0 sender on given connection, skipping processing message")
							s.Close(nil)
							linkFail += 1

							if linkFail > conn.LinkFailureLimit {
								conn.logger.Warn("Too many link failures in row, reconnecting")
								err = conn.Reconnect("out")
								if err != nil {
									conn.logger.Error("Unable to send data, shutting down sending loop")
									goto done
								}
							}
							continue
						} else {
							linkFail = 0
							senders[message.Address] = s
							sender = s
						}
					}

					counter += int64(1)
					m := amqp.NewMessageWith(message.Body)
					m.SetMessageId(counter)
					m.SetContentType("application/json")

					conn.logger.Metadata(logging.Metadata{
						"address": message.Address,
						"message": m,
					})
					conn.logger.Debug("Sending AMQP1.0 message")
					sender.SendAsyncTimeout(m, ackchan, m.MessageId(), timeout)
				default:
					conn.logger.Metadata(map[string]interface{}{
						"message": msg,
					})
					conn.logger.Debug("Skipped processing of sent AMQP1.0 message with invalid type")
				}
			case <-conn.interrupt:
				goto done
			}
		}
	done:
		for s := range senders {
			senders[s].Close(nil)
		}
	}(inchan)
}
