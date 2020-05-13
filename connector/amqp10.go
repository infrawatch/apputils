package connector

import (
	"fmt"
	"strings"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

type AMQP10Connector struct {
	Address       string
	ClientName    string
	SendTimeout   int64
	inConnection  *electron.Connection
	outConnection *electron.Connection
	receivers     []electron.Receiver
	logger        *logging.Logger
}

type AMQP10Message struct {
	Address string
	Body    string
}

//NewAMQP10Connector creates new AMQP1.0 connector from the given configuration file
func NewAMQP10Connector(cfg config.Config, logger *logging.Logger) (*AMQP10Connector, error) {
	connector := AMQP10Connector{}
	switch conf := cfg.(type) {
	case config.INIConfig:
		if addr, err := conf.GetOption("amqp1/connection"); err != nil {
			connector.Address = addr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get connection URL from configuration file at amqp1/connection")
		}
		if clientName, err := conf.GetOption("amqp1/client_name"); err != nil {
			connector.ClientName = clientName.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client name from configuration file at amqp1/client_name")
		}
		if clientName, err := conf.GetOption("amqp1/send_timeout"); err != nil {
			connector.ClientName = clientName.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get send timeout from configuration file at amqp1/send_timeout")
		}
	case config.JSONConfig:
		if addr, err := conf.GetOption("amqp1.Connection.Address"); err != nil {
			connector.Address = addr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get connection URL from configuration file at Amqp1.Connection.Address")
		}
		if addr, err := conf.GetOption("amqp1.Connection.SendTimeout"); err != nil {
			connector.Address = addr.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get send timeout from configuration file at Amqp1.Connection.SendTimeout")
		}
		if clientName, err := conf.GetOption("Amqp1.Client.Name"); err != nil {
			connector.ClientName = clientName.GetString()
		} else {
			return &connector, fmt.Errorf("Failed to get client name from configuration file at Amqp1.Client.Name")
		}
	}

	connector.receivers = make([]electron.Receiver, 0)
	return &connector, nil
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
	conn.inConnection = &cin

	outContainer := electron.NewContainer(fmt.Sprintf("%s-infrawatch-out-%d", conn.ClientName, time.Now().Unix()))
	cout, err := outContainer.Dial("tcp", url.Host)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		conn.logger.Debug("AMQP dial TCP error")
		return err
	}
	conn.outConnection = &cout

	return nil
}

//CreateReceiver creates electron.Receiver for given address
func (conn *AMQP10Connector) CreateReceiver(address string, prefetch int) error {
	addr := strings.TrimPrefix(address, "/")
	opts := []electron.LinkOption{electron.Source(addr)}
	if prefetch > 0 {
		conn.logger.Metadata(map[string]interface{}{
			"address":  address,
			"prefetch": prefetch,
		})
		conn.logger.Debug("Setting prefetch for address")
		opts = append(opts, electron.Capacity(prefetch), electron.Prefetch(true))
	}

	if rcv, err := (*conn.inConnection).Receiver(opts...); err == nil {
		conn.receivers = append(conn.receivers, rcv)
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
	(*conn.inConnection).Close(nil)
	(*conn.outConnection).Close(nil)
	conn.logger.Metadata(map[string]interface{}{
		"incoming": conn.inConnection,
		"outgoing": conn.outConnection,
	})
	conn.logger.Debug("Closed connections")
}

//Start starts all processing loops. Channel outchan will contain received AMQP10Message from AMQP1.0 node
// and through inchan AMQP10Message are sent to configured AMQP1.0 node
func (conn *AMQP10Connector) Start(outchan chan interface{}, inchan chan interface{}) {
	//create listening goroutine for each receiver
	for _, rcv := range conn.receivers {
		go func() {
			for {
				if msg, err := rcv.Receive(); err == nil {
					msg.Accept()

					message := AMQP10Message{Address: rcv.Source()}
					switch typedBody := msg.Message.Body().(type) {
					case amqp.Binary:
						message.Body = typedBody.String()
					case string:
						message.Body = typedBody
					default:
						conn.logger.Metadata(map[string]interface{}{
							"message": typedBody,
						})
						conn.logger.Info("Skipped processing of received AMQP1.0 message with invalid type")
					}
					conn.logger.Debug("Message ACKed")
					outchan <- message
				} else if err == electron.Closed {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"address":    rcv.Source(),
					})
					conn.logger.Warn("Channel closed, closing receiver loop")
					return
				} else {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"address":    rcv.Source(),
						"error":      err,
					})
					conn.logger.Error("Received AMQP1.0 error")
				}
			}
		}()
	}

	//create sending goroutine
	go func() {
		for msg := range inchan {
			switch message := msg.(type) {
			case AMQP10Message:
				sender, err := (*conn.outConnection).Sender(electron.Target(message.Address))
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
				sender.SendSyncTimeout(m, time.Duration(conn.SendTimeout))
			default:
				conn.logger.Metadata(map[string]interface{}{
					"message": msg,
				})
				conn.logger.Info("Skipped processing of sent AMQP1.0 message with invalid type")
			}
		}
	}()
}
