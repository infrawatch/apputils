package amqp10

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"

	amqp "github.com/Azure/go-amqp"
)

const (
	defaultSendTimeout      = 2
	defaultListenPrefetch   = 1
	defaultClientName       = "localhost"
	defaultLinkFailureLimit = 20
	defaultMaxParallelSend  = 128
)

// AMQP10Connector is the object to be used for communication with AMQP-1.0 entity
type AMQP10Connector struct {
	Address              string
	ClientName           string
	SendTimeout          int64
	ListenPrefetch       int64
	LinkFailureLimit     int64
	MaxParallelSendLimit int64
	appName              string
	inConnection         *amqp.Session
	outConnection        *amqp.Session
	receivers            map[string]*amqp.Receiver
	senders              map[string]*amqp.Sender
	logger               *logging.Logger
	inInterrupt          chan bool
	outInterrupt         chan bool
}

// AMQP10Message holds received (or to be sent) messages from (to) AMQP-1.0 entity
type AMQP10Message struct {
	Address string
	Body    string
	id      uint64
}

// SetIdFromCounter sets message id from shared counter
func (m *AMQP10Message) SetIdFromCounter(counter *uint64) {
	(*counter) += 1
	m.id = *counter
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
func CreateAMQP10Connector(
	logger *logging.Logger,
	address string,
	clientName string,
	appName string,
	sendTimeout int64,
	linkFailureLimit int64,
	maxParallelLimit int64,
	listenPrefetch int64,
	listenChannels []string,
) (*AMQP10Connector, error) {
	connector := AMQP10Connector{
		Address:              address,
		ClientName:           clientName,
		SendTimeout:          sendTimeout,
		ListenPrefetch:       listenPrefetch,
		LinkFailureLimit:     linkFailureLimit,
		MaxParallelSendLimit: maxParallelLimit,
		appName:              appName,
		logger:               logger,
		receivers:            make(map[string]*amqp.Receiver),
		senders:              make(map[string]*amqp.Sender),
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
		opt, err = conf.GetOption("amqp1/send_max_in_parallel")
	case *config.JSONConfig:
		opt, err = conf.GetOption("Amqp1.Connection.SendMaxInParallel")
	}
	maxParallelLimit := int64(defaultMaxParallelSend)
	if opt != nil && err == nil {
		maxParallelLimit = opt.GetInt()
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

	return CreateAMQP10Connector(logger, addr, clientName, appName, sendTimeout, linkLimit, maxParallelLimit, prf, listen)
}

//---------------------------- connect helpers and method ----------------------------

func dial(address, containerName string) (*amqp.Conn, error) {
	opts := amqp.ConnOptions{
		ContainerID: containerName,
	}
	return amqp.Dial(context.Background(), address, &opts)
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
	sess, err := c.NewSession(context.Background(), nil)
	if err != nil {
		conn.logger.Metadata(map[string]interface{}{
			"container": container,
		})
		conn.logger.Debug("Failed to create AMQP1.0 session")
		return err
	}

	switch connType {
	case "in":
		conn.inConnection = sess
		conn.inInterrupt = make(chan bool)
	case "out":
		conn.outConnection = sess
		conn.outInterrupt = make(chan bool)
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
	channel := strings.TrimPrefix(address, "/")

	if prefetch > 0 {
		conn.logger.Metadata(map[string]interface{}{
			"address":  address,
			"prefetch": prefetch,
		})
		conn.logger.Debug("Setting prefetch for address")
	}
	if prefetch < 1 {
		// credit has to be at least 1
		prefetch = 1
	}
	opts := amqp.ReceiverOptions{
		Credit: int32(prefetch),
	}

	rcv, err := conn.inConnection.NewReceiver(context.Background(), channel, &opts)
	if err != nil {
		conn.logger.Metadata(logging.Metadata{
			"address": channel,
			"error":   err,
		})
		conn.logger.Debug("Failed to create receiver for given address")
		return err
	}

	conn.receivers[channel] = rcv
	return nil
}

// CreateSender creates electron.Sender for given address
func (conn *AMQP10Connector) CreateSender(address string) (*amqp.Sender, error) {
	channel := strings.TrimPrefix(address, "/")
	if s, ok := conn.senders[channel]; ok {
		s.Close(context.Background())
		delete(conn.senders, channel)
	}

	opts := amqp.SenderOptions{
		RequestedReceiverSettleMode: amqp.ReceiverSettleModeFirst.Ptr(),
	}

	snd, err := conn.outConnection.NewSender(context.Background(), address, &opts)
	if err != nil {
		conn.logger.Metadata(logging.Metadata{
			"address": fmt.Sprintf("%s/%s", conn.Address, channel),
			"error":   err,
		})
		conn.logger.Warn("Failed to create sender for given address")
		return nil, fmt.Errorf("Failed to create sender")
	}

	conn.senders[channel] = snd
	return snd, nil
}

//---------------------------- reconnect helpers and method ---------------------------

func (conn *AMQP10Connector) processIncomingMessage(msg []byte, outchan chan interface{}, receiver *amqp.Receiver) {
	message := AMQP10Message{
		Address: receiver.Address(),
		Body:    string(msg),
	}
	outchan <- message
}

func (conn *AMQP10Connector) startSenders(inchan chan interface{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func(conn *AMQP10Connector, inchan chan interface{}) {
		defer wg.Done()

		lfLock := sync.RWMutex{}
		sndLock := sync.RWMutex{}

		// number of link failures
		linkFail := int64(0)
		// count of active sending coroutines
		activeSend := int64(0)

		counter := uint64(0)
		for {
			lfLock.RLock()
			failure := linkFail > conn.LinkFailureLimit
			lfLock.RUnlock()

			if failure {
				conn.logger.Warn("Too many link failures in row, reconnecting")
				goto reconnectSend
			}

			select {
			case msg := <-inchan:
				switch message := msg.(type) {
				case AMQP10Message:
					var sender *amqp.Sender
					if s, ok := conn.senders[message.Address]; ok {
						sender = s
					} else {
						if s, err := conn.CreateSender(message.Address); err != nil {
							conn.logger.Metadata(logging.Metadata{
								"cause":   "creating sender failed",
								"reason":  err,
								"address": message.Address,
							})
							conn.logger.Warn("Skipping processing message")

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

					// block if we have too much sending goroutines
					sndLock.RLock()
					for activeSend > conn.MaxParallelSendLimit {
						time.Sleep(time.Second)
					}
					sndLock.RUnlock()

					// send message
					sndLock.Lock()
					activeSend += int64(1)
					sndLock.Unlock()
					message.SetIdFromCounter(&counter)
					go func(sender *amqp.Sender, msg AMQP10Message, sndLock *sync.RWMutex, lfLock *sync.RWMutex, timeout time.Duration) {
						var (
							connErr *amqp.ConnError
							linkErr *amqp.LinkError
							sessErr *amqp.SessionError
						)

						ctx, cancel := context.WithTimeout(context.Background(), timeout)
						defer func(cancel context.CancelFunc) {
							cancel()
							sndLock.Lock()
							activeSend -= int64(1)
							sndLock.Unlock()
						}(cancel)

						ctype := "application/json"
						prop := amqp.MessageProperties{
							MessageID:   msg.id,
							ContentType: &ctype,
						}
						m := amqp.Message{
							Properties: &prop,
							Data:       [][]byte{[]byte(msg.Body)},
						}

						msgMeta := logging.Metadata{
							"id":      msg.id,
							"address": msg.Address,
						}
						conn.logger.Metadata(msgMeta)
						conn.logger.Debug("Sending message")

						err := sender.Send(ctx, &m, nil)
						if err != nil {
							lfLock.Lock()
							linkFail += 1
							lfLock.Unlock()
							if urr, ok := err.(*amqp.Error); ok {
								msgMeta["reason"] = urr.Description
							} else if errors.As(err, &connErr) && connErr.RemoteErr != nil {
								msgMeta["reason"] = connErr.RemoteErr.Description
							} else if errors.As(err, &linkErr) && linkErr.RemoteErr != nil {
								msgMeta["reason"] = linkErr.RemoteErr.Description
							} else if errors.As(err, &sessErr) && sessErr.RemoteErr != nil {
								msgMeta["reason"] = sessErr.RemoteErr.Description
							}
							conn.logger.Metadata(msgMeta)
							conn.logger.Warn("Failed to send message")

							msgMeta["message"] = msg.Body
							msgMeta["error"] = err
							conn.logger.Metadata(msgMeta)
							conn.logger.Debug("Send error debug details")
						}
					}(sender, message, &sndLock, &lfLock, time.Duration(conn.SendTimeout)*time.Second)

				default:
					conn.logger.Metadata(logging.Metadata{
						"message": msg,
					})
					conn.logger.Debug("Skipped processing of sent AMQP1.0 message with invalid type")
				}
			case <-conn.outInterrupt:
				goto doneSend
			}
		}

	reconnectSend:
		if err := conn.Reconnect("out", inchan, wg); err != nil {
			conn.logger.Metadata(logging.Metadata{
				"error": err,
			})
			conn.logger.Error("Failed to reconnect sending loop")
		}

	doneSend:
		conn.logger.Info("Shutting down sending loop")

	}(conn, inchan)
}

func (conn *AMQP10Connector) stopSenders() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	for s := range conn.senders {
		conn.senders[s].Close(ctx)
		conn.logger.Metadata(map[string]interface{}{
			"sender": conn.senders[s].Address(),
		})
		conn.logger.Debug("Closed sender link")
	}
	conn.senders = map[string]*amqp.Sender{}
}

func (conn *AMQP10Connector) startReceivers(outchan chan interface{}, wg *sync.WaitGroup) {
	for _, rcv := range conn.receivers {
		wg.Add(1)
		go func(receiver *amqp.Receiver) {
			var (
				connErr *amqp.ConnError
				linkErr *amqp.LinkError
				sessErr *amqp.SessionError
			)

			defer wg.Done()

			connLogMeta := logging.Metadata{
				"connection": conn.Address,
				"address":    receiver.Address(),
			}
			conn.logger.Metadata(connLogMeta)
			conn.logger.Warn("Created receiver")
			// number of link failures
			linkFail := int64(0)
			for {
				select {
				case <-conn.inInterrupt:
					goto doneReceive
				default:
					ctx, _ := context.WithTimeout(context.Background(), time.Second)
					if msg, err := receiver.Receive(ctx, nil); err == nil {
						receiver.AcceptMessage(context.Background(), msg)
						conn.processIncomingMessage(msg.GetData(), outchan, receiver)
						conn.logger.Debug("Message ACKed")
						linkFail = int64(0)
					} else if errors.As(err, &connErr) || errors.As(err, &linkErr) || errors.As(err, &sessErr) {
						linkFail += int64(1)
						if linkFail > conn.LinkFailureLimit {
							conn.logger.Metadata(connLogMeta)
							conn.logger.Warn("Too many link failures in row, reconnecting")
							goto reconnectReceive
						}
					} else {
						// receiver wait timeouted
					}
				}
			}

		reconnectReceive:
			if err := conn.Reconnect("in", outchan, wg); err != nil {
				conn.logger.Metadata(logging.Metadata{
					"error": err,
				})
				conn.logger.Error("Failed to reconnect receiver loop")
			}

		doneReceive:
			conn.logger.Metadata(connLogMeta)
			conn.logger.Info("Shutting down receiver loop")
		}(rcv)
	}
}

func (conn *AMQP10Connector) stopReceivers() {
	ctx, _ := context.WithTimeout(context.Background(), time.Second)
	for r := range conn.receivers {
		conn.receivers[r].Close(ctx)
		conn.logger.Metadata(map[string]interface{}{
			"receiver": conn.receivers[r].Address(),
		})
		conn.logger.Debug("Closed receiver link")
	}
	conn.receivers = map[string]*amqp.Receiver{}
}

// Reconnect tries to reconnect connector to configured AMQP1.0 node. Returns nil if failed
func (conn *AMQP10Connector) Reconnect(connectionType string, msgChannel chan interface{}, wg *sync.WaitGroup) error {
	ctx := context.Background()
	listen := []string{}
	switch connectionType {
	case "in":
		close(conn.inInterrupt)
		for r := range conn.receivers {
			listen = append(listen, conn.receivers[r].Address())
		}
		conn.stopReceivers()
		conn.inConnection.Close(ctx)
		conn.logger.Debug("Disconnected incoming connection")
	case "out":
		close(conn.outInterrupt)
		conn.stopSenders()
		conn.outConnection.Close(ctx)
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

	switch connectionType {
	case "in":
		if err := bindListenChannels(conn, listen); err != nil {
			return fmt.Errorf("Error while creating receiver links")
		}
		conn.logger.Metadata(logging.Metadata{
			"receivers": conn.receivers,
		})
		conn.logger.Debug("Recreated receiver links")
		conn.startReceivers(msgChannel, wg)
	case "out":
		conn.startSenders(msgChannel, wg)
	}
	return nil
}

// Disconnect closes connection in both directions
func (conn *AMQP10Connector) Disconnect() {
	ctx := context.Background()
	close(conn.inInterrupt)
	close(conn.outInterrupt)
	time.Sleep(time.Second)
	conn.inConnection.Close(ctx)
	conn.outConnection.Close(ctx)
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
	conn.startReceivers(outchan, &wg)
	conn.startSenders(inchan, &wg)
	return &wg
}
