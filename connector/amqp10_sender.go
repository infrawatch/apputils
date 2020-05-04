/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package connector

import (
	"fmt"
	"os"
	"strings"

	"github.com/infrawatch/apputils/logging"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

//AMQPSender msgcount -1 is infinite
type AMQPSender struct {
	urlStr      string
	connections chan electron.Connection
	acks        chan electron.Outcome
	debug       bool
	logger      *logging.Logger
}

//NewAMQPSender   ...
func NewAMQPSender(urlStr string, debug bool, logger *logging.Logger) *AMQPSender {
	if len(urlStr) == 0 {
		logger.Error("No URL provided")
		os.Exit(1)
	}
	server := &AMQPSender{
		urlStr:      urlStr,
		connections: make(chan electron.Connection, 1),
		acks:        make(chan electron.Outcome),
		debug:       debug,
		logger:      logger,
	}

	return server
}

//Close connections it is exported so users can force close
func (as *AMQPSender) Close() {
	c := <-as.connections
	c.Close(nil)
	as.logger.Metadata(map[string]interface{}{
		"connection": c,
	})
	as.logger.Debug("close sender connection")
}

// GetAckChannel returns electron.Outcome channel for receiving ACK when debug mode is turned on
func (as *AMQPSender) GetAckChannel() chan electron.Outcome {
	return as.acks
}

//Send  starts amqp server
func (as *AMQPSender) Send(jsonmsg string) {
	as.logger.Debug("AMQP send is invoked")
	go func(body string) {
		container := electron.NewContainer(fmt.Sprintf("send[%v]", os.Getpid()))
		url, err := amqp.ParseURL(as.urlStr)
		fatalsIf(err, as.logger)
		c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
		fatalsIf(err, as.logger)
		as.connections <- c // Save connection so we can Close() when start() ends
		addr := strings.TrimPrefix(url.Path, "/")
		s, err := c.Sender(electron.Target(addr))
		fatalsIf(err, as.logger)

		m := amqp.NewMessage()
		m.SetContentType("application/json")
		m.Marshal(body)

		as.logger.Metadata(map[string]interface{}{
			"body": body,
		})
		as.logger.Debug("Sending alerts on a bus URL")

		if as.debug {
			s.SendAsync(m, as.acks, "smart-gateway-ack")
		} else {
			s.SendForget(m)
		}
		as.Close()
	}(jsonmsg)
}

func fatalsIf(err error, logger *logging.Logger) {
	if err != nil {
		logger.Metadata(map[string]interface{}{
			"error": err,
		})
		logger.Error("Error occured")
	}
}
