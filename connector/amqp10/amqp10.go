package amqp10

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"

	"github.com/apache/qpid-proton/go/pkg/amqp"
	"github.com/apache/qpid-proton/go/pkg/electron"
)

const (
	defaultSendTimeout      = 2
	defaultListenPrefetch   = -1
	defaultClientName       = "localhost"
	defaultLinkFailureLimit = 20
)

// AMQP10Receiver is tagged electron receiver
type AMQP10Receiver struct {
	Receiver electron.Receiver
	Tags     []string
}

// AMQP10Connector is the object to be used for communication with AMQP-1.0 entity
type AMQP10Connector struct {
	Address          string
	ClientName       string
	SendTimeout      int64
	ListenPrefetch   int64
	LinkFailureLimit int64
	appName          string
	inConnection     electron.Connection
	outConnection    electron.Connection
	receivers        []AMQP10Receiver
	senders          map[string]electron.Sender
	logger           *logging.Logger
	interrupt        chan bool
}

// AMQP10Message holds received (or to be sent) messages from (to) AMQP-1.0 entity
type AMQP10Message struct {
	Address string
	Body    string
	Tags    []string
}

//-------------------------- constructors and their helpers --------------------------

func bindListenChannels(conn *AMQP10Connector, listen []string) error {
	for _, channel := range listen {
		if len(channel) < 1 {
			continue
		}
		conn.logger.Metadata(logging.Metadata{
			"channel":  channel,
			"prefetch": conn.ListenPrefetch,
		})
		conn.logger.Debug("Creating AMQP receiver for channel")
		if err := conn.CreateReceiver(channel, int(conn.ListenPrefetch)); err != nil {
			return fmt.Errorf("Failed to create receiver: %s", err)
		}
	}
	return nil
}

// CreateAMQP10Connector creates the connector and connects to given AMQP1.0 service
func CreateAMQP10Connector(logger *logging.Logger, address string, clientName string,
	appName string, sendTimeout int64, linkFailureLimit int64, listenPrefetch int64,
	listenChannels []string) (*AMQP10Connector, error) {
	connector := AMQP10Connector{
		Address:          address,
		ClientName:       clientName,
		SendTimeout:      sendTimeout,
		ListenPrefetch:   listenPrefetch,
		LinkFailureLimit: linkFailureLimit,
		appName:          appName,
		logger:           logger,
		receivers:        make([]AMQP10Receiver, 0),
		senders:          make(map[string]electron.Sender),
		interrupt:        make(chan bool),
	}

	// connect
	if err := connector.Connect(); err != nil {
		return &connector, fmt.Errorf("Error while connecting to AMQP")
	}
	// bind to channels
	if err := bindListenChannels(&connector, listenChannels); err != nil {
		return &connector, fmt.Errorf("Error while creating receivers")
	}
	return &connector, nil
}

// ConnectAMQP10 creates new AMQP1.0 connector from the given configuration file
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
	linkLimit := int64(defaultLinkFailureLimit)
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

//---------------------------- connect helpers and method ----------------------------

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
	container := fmt.Sprintf("%s-%s-%s-%d", conn.ClientName, conn.appName, connType, time.Now().Unix()-int64(rand.Intn(10)))
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

// Connect creates input and output connection container for given appname for configured AMQP1.0 node
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

// CreateReceiver creates electron.Receiver for given address
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

// CreateSender creates electron.Sender for given address
func (conn *AMQP10Connector) CreateSender(address string) (*electron.Sender, error) {
	channel := strings.TrimPrefix(address, "/")
	if s, ok := conn.senders[channel]; ok {
		s.Close(nil)
		delete(conn.senders, channel)
	}

	var err error
	var snd electron.Sender
	opts := []electron.LinkOption{electron.Target(address), electron.AtMostOnce()}
	if snd, err = conn.outConnection.Sender(opts...); err != nil {
		conn.logger.Metadata(logging.Metadata{
			"address": fmt.Sprintf("%s/%s", conn.Address, channel),
			"error":   err,
		})
		conn.logger.Warn("Failed to create sender for given address")
		return nil, fmt.Errorf("Failed to create sender")
	}
	conn.senders[channel] = snd
	return &snd, nil
}

//---------------------------- reconnect helpers and method ---------------------------

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

func (conn *AMQP10Connector) startReceivers(outchan chan interface{}, wg *sync.WaitGroup) {
	for _, rcv := range conn.receivers {
		wg.Add(1)
		go func(receiver AMQP10Receiver) {
			defer wg.Done()
			conn.logger.Metadata(logging.Metadata{
				"connection": conn.Address,
				"address":    receiver.Receiver.Source(),
			})
			conn.logger.Warn("Created receiver")
			for {
				select {
				case <-conn.interrupt:
					goto doneReceive
				default:
				}
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
					goto doneReceive
				} else {
					conn.logger.Metadata(map[string]interface{}{
						"connection": conn.Address,
						"address":    receiver.Receiver.Source(),
						"error":      err,
					})
					conn.logger.Error("Received AMQP1.0 error")
				}
			}

		doneReceive:
			conn.logger.Metadata(map[string]interface{}{
				"connection": conn.Address,
				"address":    receiver.Receiver.Source(),
			})
			conn.logger.Error("Shutting down receiver")
		}(rcv)
	}
}

// Reconnect tries to reconnect connector to configured AMQP1.0 node. Returns nil if failed
func (conn *AMQP10Connector) Reconnect(connectionType string, outchan chan interface{}, wg *sync.WaitGroup) error {
	listen := []string{}
	switch connectionType {
	case "in":
		for r := range conn.receivers {
			// get receiver data
			tags := strings.Join(conn.receivers[r].Tags, ":")
			address := conn.receivers[r].Receiver.Source()
			if len(tags) > 0 {
				address = strings.Join([]string{address, tags}, ":")
			}
			listen = append(listen, address)
			// close receiver
			conn.receivers[r].Receiver.Close(nil)
			for {
				if err := conn.receivers[r].Receiver.Sync(); err == electron.Closed {
					break
				} else {
					conn.logger.Metadata(map[string]interface{}{
						"receiver": conn.receivers[r].Receiver,
					})
					conn.logger.Debug("Waiting for receiver to be closed")
					time.Sleep(time.Millisecond)
				}
			}
			conn.logger.Metadata(map[string]interface{}{
				"receiver": conn.receivers[r].Receiver,
			})
			conn.logger.Debug("Closed receiver link")
		}
		conn.receivers = []AMQP10Receiver{}

		conn.inConnection.Disconnect(fmt.Errorf("Reconnecting"))
		conn.inConnection.Wait()
		conn.logger.Debug("Disconnected incoming connection")
	case "out":
		for s := range conn.senders {
			conn.senders[s].Close(fmt.Errorf("Reconnecting"))
			delete(conn.senders, s)
			conn.logger.Metadata(map[string]interface{}{
				"sender": conn.senders[s],
			})
			conn.logger.Debug("Closed sender link")
		}
		conn.outConnection.Disconnect(fmt.Errorf("Reconnecting"))
		conn.outConnection.Wait()
		conn.logger.Debug("Disconnected outgoing connection")
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

	if connectionType == "in" {
		if err := bindListenChannels(conn, listen); err != nil {
			return fmt.Errorf("Error while creating receiver links")
		}
		conn.logger.Metadata(map[string]interface{}{
			"receivers": conn.receivers,
		})
		conn.logger.Debug("Recreated receiver links")
		// recreate receiving loops
		conn.startReceivers(outchan, wg)
	}
	return nil
}

// Disconnect closes connection in both directions
func (conn *AMQP10Connector) Disconnect() {
	close(conn.interrupt)
	time.Sleep(time.Second)
	conn.inConnection.Disconnect(nil)
	conn.outConnection.Disconnect(nil)
	conn.logger.Metadata(map[string]interface{}{
		"incoming": conn.inConnection,
		"outgoing": conn.outConnection,
	})
	conn.logger.Debug("Closed connections")
}

//---------------------------- message processing initiation method ----------------------------

// Start starts all processing loops. Channel outchan will contain received AMQP10Message from AMQP1.0 node
// and through inchan AMQP10Message are sent to configured AMQP1.0 node
func (conn *AMQP10Connector) Start(outchan chan interface{}, inchan chan interface{}) *sync.WaitGroup {
	wg := sync.WaitGroup{}

	//create listening goroutine for each receiver
	conn.startReceivers(outchan, &wg)

	ackchan := make(chan electron.Outcome)
	linkFail := int64(0)
	lfLock := sync.RWMutex{}

	// ACK and error verification goroutine
	wg.Add(1)
	go func(ackchan chan electron.Outcome) {
		defer wg.Done()
		for {
			select {
			case ack := <-ackchan:
				if ack.Error != nil {
					lfLock.Lock()
					linkFail += 1
					lfLock.Unlock()
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
					lfLock.Lock()
					linkFail = 0
					lfLock.Unlock()
					conn.logger.Metadata(logging.Metadata{
						"id": ack.Value,
					})
					conn.logger.Debug("Sent message ACKed.")
				}
			case <-conn.interrupt:
				goto doneAck
			}
		}
	doneAck:
		conn.logger.Debug("Shutting down ACK check coroutine.")
	}(ackchan)

	//create sending goroutine
	wg.Add(1)
	go func(inchan chan interface{}) {

		defer wg.Done()

		timeout := time.Duration(conn.SendTimeout) * time.Second
		counter := int64(0)
		for {
			lfLock.RLock()
			failure := linkFail > conn.LinkFailureLimit
			lfLock.RUnlock()

			if failure {
				conn.logger.Warn("Too many link failures in row, reconnecting")
				err := conn.Reconnect("out", outchan, &wg)
				if err != nil {
					conn.logger.Metadata(logging.Metadata{
						"error": err,
					})
					conn.logger.Error("Unable to send data, shutting down sending loop")
					goto doneSend
				}
			}

			select {
			case msg := <-inchan:
				switch message := msg.(type) {
				case AMQP10Message:
					var sender *electron.Sender
					if s, ok := conn.senders[message.Address]; ok {
						sender = &s
					} else {
						if s, err := conn.CreateSender(message.Address); err != nil {
							conn.logger.Metadata(logging.Metadata{
								"reason": err,
							})
							conn.logger.Warn("Skipping processing message")
							(*s).Close(nil)

							lfLock.Lock()
							linkFail += 1
							lfLock.Unlock()
							continue
						} else {
							lfLock.Lock()
							linkFail = 0
							lfLock.Unlock()
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
					(*sender).SendAsyncTimeout(m, ackchan, m.MessageId(), timeout)
				default:
					conn.logger.Metadata(map[string]interface{}{
						"message": msg,
					})
					conn.logger.Debug("Skipped processing of sent AMQP1.0 message with invalid type")
				}
			case <-conn.interrupt:
				goto doneSend
			}
		}
	doneSend:
		conn.logger.Debug("Shutting down sending coroutine.")
		for s := range conn.senders {
			conn.senders[s].Close(nil)
		}
	}(inchan)

	return &wg
}
