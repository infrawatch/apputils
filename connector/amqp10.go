package connector

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
)

//AMQP10Receiver is tagged electron receiver
type AMQP10Receiver struct {
	Receiver electron.Receiver
	Tags     []string
}

//AMQP10Connector is the object to be used for communication with AMQP-1.0 entity
type AMQP10Connector struct {
	Address       string
	ClientName    string
	SendTimeout   int64
	inConnection  electron.Connection
	outConnection electron.Connection
	receivers     []AMQP10Receiver
	logger        *logging.Logger
}

//AMQP10Message holds received (or to be sent) messages from (to) AMQP-1.0 entity
type AMQP10Message struct {
	Address string
	Body    string
	Tags    []string
}

//CreateAMQP10Connector creates the connector and connects to given AMQP1.0 service
func CreateAMQP10Connector(logger *logging.Logger, address string, clientName string, sendTimeout int64, listenPrefetch int64, listenChannels []string) (*AMQP10Connector, error) {
	connector := AMQP10Connector{
		Address:     address,
		ClientName:  clientName,
		SendTimeout: sendTimeout,
		logger:      logger,
		receivers:   make([]AMQP10Receiver, 0),
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
func ConnectAMQP10(cfg config.Config, logger *logging.Logger) (*AMQP10Connector, error) {
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
	if opt == nil {
		return nil, fmt.Errorf("Failed to get connection URL from configuration file")
	}
	addr := opt.GetString()

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/send_timeout")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.SendTimeout")
	}
	if err != nil {
		return nil, err
	}
	sendTimeout := int64(defaultSendTimeout)
	if opt != nil {
		sendTimeout = opt.GetInt()
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/client_name")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Client.Name")
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
		opt, err = conf.GetOption("amqp1/listen_channels")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.ListenChannels")
	}
	if err != nil {
		return nil, err
	}
	listen := []string{}
	if opt != nil {
		listen = opt.GetStrings(",")
	}

	switch conf := cfg.(type) {
	case *config.INIConfig:
		opt, err = conf.GetOption("amqp1/listen_prefetch")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.ListenPrefetch")
	}
	if err != nil {
		return nil, err
	}
	prf := int64(defaultListenPrefetch)
	if opt != nil {
		prf = opt.GetInt()
	}

	return CreateAMQP10Connector(logger, addr, clientName, sendTimeout, prf, listen)
}

//Connect creates input and output connection to configured AMQP1.0 node
func (conn *AMQP10Connector) Connect() error {
	url, err := amqp.ParseURL(conn.Address)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error":      err,
			"connection": conn.Address,
		})
		conn.logger.Debug("Error while parsing AMQP1.0 URL")
		return err
	}

	inContainer := electron.NewContainer(fmt.Sprintf("%s-infrawatch-in-%d", conn.ClientName, time.Now().Unix()))
	cin, err := inContainer.Dial("tcp", url.Host)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		conn.logger.Debug("AMQP dial TCP error")
		return err
	}
	conn.inConnection = cin

	outContainer := electron.NewContainer(fmt.Sprintf("%s-infrawatch-out-%d", conn.ClientName, time.Now().Unix()))
	cout, err := outContainer.Dial("tcp", url.Host)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		conn.logger.Debug("AMQP dial TCP error")
		return err
	}
	conn.outConnection = cout

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
func (conn *AMQP10Connector) Reconnect() error {

	return nil
}

//Disconnect closes all connections
func (conn *AMQP10Connector) Disconnect() {
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
	go func() {
		for msg := range inchan {
			switch message := msg.(type) {
			case AMQP10Message:
				sender, err := conn.outConnection.Sender(electron.Target(message.Address))
				if err != nil {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"message":    message,
						"error":      err,
					})
					conn.logger.Warn("Failed to create AMQP1.0 sender on given connection, skipping processing message")
					continue
				}
				conn.logger.Metadata(map[string]interface{}{
					"address": message.Address,
					"body":    message.Body,
				})
				conn.logger.Debug("Sending AMQP1.0 message")

				m := amqp.NewMessage()
				m.SetContentType("application/json")
				m.Marshal(message.Body)

				ackChan := sender.SendWaitable(m)
				timer := time.NewTimer(time.Duration(conn.SendTimeout) * time.Second)

				select {
				case ack := <-ackChan:
					if ack.Status != 2 {
						conn.logger.Metadata(map[string]interface{}{
							"message": m,
							"ack":     ack,
						})
						conn.logger.Warn("Sent message was not ACKed")
					}
				case <-timer.C:
					conn.logger.Metadata(map[string]interface{}{
						"message": m,
					})
					conn.logger.Warn("Sent message timed out on ACK. Delivery not guaranteed.")
				}
			default:
				conn.logger.Metadata(map[string]interface{}{
					"message": msg,
				})
				conn.logger.Debug("Skipped processing of sent AMQP1.0 message with invalid type")
			}
		}
	}()
}
