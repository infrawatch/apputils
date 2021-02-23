package connector

import (
	"fmt"
	"net"
	"os"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
)

const maxBufferSize = 4096

type socketInfo struct {
	Address net.UnixAddr
	Pc      *net.UnixConn
}

type UnixSocketConnector struct {
	msgBuffer []byte
	logger    *logging.Logger
	out       *socketInfo
	in        *socketInfo
}

// NOTE: The connector creates the file for the incoming socket
// and expects the file to already be created for the outgoing
// socket. Both or only one of the directions can be configured.

//CreateUnixSocketConnector ...
func CreateUnixSocketConnector(logger *logging.Logger, inAddress string, outAddress string, maxBufferSize uint64) (*UnixSocketConnector, error) {
	connector := UnixSocketConnector{
		msgBuffer: make([]byte, maxBufferSize),
		logger:    logger,
		out:       &socketInfo{},
		in:        &socketInfo{},
	}

	if inAddress != "" {
		connector.in.Address.Name = inAddress
		connector.in.Address.Net = "unixgram"
		connector.logger.Metadata(map[string]interface{}{
			"address": connector.in.Address.Name,
		})
		connector.logger.Debug("In socket configured")
	} else {
		connector.in = nil
		connector.logger.Debug("The in socket isn't configured")
	}

	if outAddress != "" {
		connector.out.Address.Name = outAddress
		connector.out.Address.Net = "unixgram"

		connector.logger.Metadata(map[string]interface{}{
			"address": connector.out.Address.Name,
		})
		connector.logger.Debug("Out socket configured")
	} else {
		connector.out = nil
		connector.logger.Debug("The out socket isn't configured")
	}

	err := connector.Connect()
	return &connector, err
}

//ConnectUnixSocket ...
func ConnectUnixSocket(cfg config.Config, logger *logging.Logger) (*UnixSocketConnector, error) {
	var err error
	var inAddress, outAddress string

	var inAddr *config.Option
	switch conf := cfg.(type) {
	case *config.INIConfig:
		inAddr, err = conf.GetOption("socket/in_address")
	case *config.JSONConfig:
		inAddr, err = conf.GetOption("Socket.In.Address")
	}

	if err == nil && inAddr != nil {
		inAddress = inAddr.GetString()
	} else {
		inAddress = ""
	}

	var outAddr *config.Option
	switch conf := cfg.(type) {
	case *config.INIConfig:
		outAddr, err = conf.GetOption("socket/out_address")
	case *config.JSONConfig:
		outAddr, err = conf.GetOption("Socket.Out.Address")
	}

	if err == nil && inAddr != nil {
		outAddress = outAddr.GetString()
	} else {
		outAddress = ""
	}

	if outAddress == "" && inAddress == "" {
		return nil, fmt.Errorf("No socket was configured")
	}

	return CreateUnixSocketConnector(logger, inAddress, outAddress, maxBufferSize)
}

func (connector *UnixSocketConnector) connectSingleSocket(info *socketInfo, isIn bool) error {
	var err error
	if isIn {
		os.Remove(info.Address.Name)
		info.Pc, err = net.ListenUnixgram("unixgram", &info.Address)
	} else {
		info.Pc, err = net.DialUnix("unixgram", nil, &info.Address)
	}
	if err != nil {
		return err
	}
	connector.logger.Metadata(map[string]interface{}{
		"address": info.Address.Name,
	})
	connector.logger.Debug("Connected to unix socket")
	return nil
}

func (connector *UnixSocketConnector) Connect() error {
	var err error
	if connector.in != nil {
		err = connector.connectSingleSocket(connector.in, true)
	}
	if connector.out != nil && err == nil {
		err = connector.connectSingleSocket(connector.out, false)
	}
	return err
}

func (connector *UnixSocketConnector) Disconnect() {
	if connector.out != nil {
		connector.out.Pc.Close()
	}
	if connector.in != nil {
		os.Remove(connector.in.Address.Name)
	}
}

func (connector *UnixSocketConnector) Start(outchan chan interface{}, inchan chan interface{}) {
	// receiving
	if connector.in != nil {
		go func() {
			for {
				n, err := connector.in.Pc.Read(connector.msgBuffer[:])
				if err != nil || n < 1 {
					connector.logger.Metadata(map[string]interface{}{
						"error":           err,
						"characters read": n,
					})
					connector.logger.Debug("Error while trying to read from unix socket.")
					continue
				}
				msg := string(connector.msgBuffer[:n])
				outchan <- msg
				connector.logger.Metadata(map[string]interface{}{
					"message": msg,
				})
				connector.logger.Debug("Recieved a message.")
			}
		}()
	}

	// sending
	if connector.out != nil {
		go func() {
			for msg := range inchan {
				switch message := msg.(type) {
				case string:
					n, err := connector.out.Pc.Write([]byte(message))
					if err != nil || n < 1 {
						connector.logger.Metadata(map[string]interface{}{
							"error":              err,
							"characters written": n,
						})
						connector.logger.Debug("Error while trying to write to unix socket.")
						continue
					}
					connector.logger.Metadata(map[string]interface{}{
						"message": message,
					})
					connector.logger.Debug("Sent a message.")
				default:
					connector.logger.Metadata(map[string]interface{}{
						"message": msg,
					})
					connector.logger.Debug("Skipped processing of sent message with invalid type")
				}
			}
		}()
	}
}
