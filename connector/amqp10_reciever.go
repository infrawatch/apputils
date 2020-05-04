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

//AMQPServer msgcount -1 is infinite
type AMQPServer struct {
	urlStr			string
	msgcount		int
	notifier		chan string
	status			chan int
	done			chan bool
	connection		electron.Connection
	method			func(s *AMQPServer) (electron.Receiver, error)
	prefetch		int
	uniqueName		string
	reconnect		bool
	collectinterval float64
	logger			*logging.Logger
}

//NewAMQPServer   ...
func NewAMQPServer(urlStr string, msgcount int, prefetch int, uniqueName string, logger *logging.Logger) *AMQPServer {
	if len(urlStr) == 0 {
		logger.Error("No URL provided")
		os.Exit(1)
	}
	server := &AMQPServer{
		urlStr:			 urlStr,
		notifier:		 make(chan string),
		status:			 make(chan int),
		done:			 make(chan bool),
		msgcount:		 msgcount,
		method:			 (*AMQPServer).connect,
		prefetch:		 prefetch,
		uniqueName:		 uniqueName,
		reconnect:		 false,
		collectinterval: 30,
		logger:			 logger,
	}

	// Spawn off the server's main loop immediately
	// not exported
	go server.start()

	return server
}

//GetNotifier  Get notifier
func (s *AMQPServer) GetNotifier() chan string {
	return s.notifier
}

//GetStatus  Get Status
func (s *AMQPServer) GetStatus() chan int {
	return s.status
}

//GetDoneChan ...
func (s *AMQPServer) GetDoneChan() chan bool {
	return s.done
}

//Close connections it is exported so users can force close
func (s *AMQPServer) Close() {
	s.connection.Close(nil)
	s.logger.Metadata(map[string]interface{}{
		"connection": s.connection,
	})
	s.logger.Debug("Close receiver connection")
}

//UpdateMinCollectInterval ...
func (s *AMQPServer) UpdateMinCollectInterval(interval float64) {
	if interval < s.collectinterval {
		s.collectinterval = interval
	}
}

//start  starts amqp server
func (s *AMQPServer) start() {
	msgBuffCount := 10
	if s.msgcount > 0 {
		msgBuffCount = s.msgcount
	}
	messages := make(chan amqp.Message, msgBuffCount) // Channel for messages from goroutines to main()
	connectionStatus := make(chan int)
	done := make(chan bool)

	defer close(done)
	defer close(messages)
	defer close(connectionStatus)

	go func() {
		r, err := s.method(s)
		if err != nil {
			s.logger.Metadata(map[string]interface{}{
				"error": err,
			})
			s.logger.Error("Could not connect to Qpid-dispatch router. is it running?")
		}
		connectionStatus <- 1
		untilCount := s.msgcount
	theloop:
		for {
			if rm, err := r.Receive(); err == nil {
				rm.Accept()
				s.logger.Metadata(map[string]interface{}{
					"message": rm.Message,
				})
				s.logger.Debug("Message ACKed")
				messages <- rm.Message
			} else if err == electron.Closed {
				s.logger.Info("Channel closed...")
				return
			} else {
				s.logger.Metadata(map[string]interface{}{
					"URL": s.urlStr,
					"error": err,
				})
				s.logger.Error("Received error")
			}
			if untilCount > 0 {
				untilCount--
			}
			if untilCount == 0 {
				break theloop
			}
		}
		done <- true
		s.done <- true
		s.Close()
		s.logger.Info("Closed AMQP...letting loop know")
	}()

msgloop:
	for {
		select {
		case <-done:
			s.logger.Debug("Done received...")
			break msgloop
		case m := <-messages:
			s.logger.Metadata(map[string]interface{}{
				"Message": m.Body(),
			})
			s.logger.Debug("Message received...")
			switch msg := m.Body().(type) {
			case amqp.Binary:
				s.notifier <- msg.String()
			case string:
				s.notifier <- msg
			default:
				// do nothing and report
				s.logger.Metadata(map[string]interface{}{
					"Message": msg,
				})
				s.logger.Info("Invalid type of AMQP message received")
			}
		case status := <-connectionStatus:
			s.logger.Debug("Status received...")
			s.status <- status
		}
	}
}

//connect Connect to an AMQP server returning an electron receiver
func (s *AMQPServer) connect() (electron.Receiver, error) {
	// Wait for one goroutine per URL
	// Make name unique-ish
	container := electron.NewContainer(fmt.Sprintf("rcv[%v]", s.uniqueName))
	url, err := amqp.ParseURL(s.urlStr)
	if err != nil {
		s.logger.Metadata(map[string]interface{}{
			"error": err,
			"url": s.urlStr,
		})
		s.logger.Error("Error while parsing amqp url")
	}
	c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	if err != nil {
		s.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		s.logger.Error("AMQP Dial tcp error")
		return nil, err
	}

	s.connection = c // Save connection so we can Close() when start() ends

	addr := strings.TrimPrefix(url.Path, "/")
	opts := []electron.LinkOption{electron.Source(addr)}
	if s.prefetch > 0 {
		s.logger.Metadata(map[string]interface{}{
			"prefetch": s.prefetch,
		})
		s.logger.Debug("Amqp Prefetch set to:")
		opts = append(opts, electron.Capacity(s.prefetch), electron.Prefetch(true))
	}

	r, err := c.Receiver(opts...)
	return r, err
}

