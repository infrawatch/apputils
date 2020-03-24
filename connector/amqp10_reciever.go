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
	"log"
	"os"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

var debugr = func(format string, data ...interface{}) {} // Default no debugging output

//AMQPServer msgcount -1 is infinite
type AMQPServer struct {
	urlStr          string
	debug           bool
	msgcount        int
	notifier        chan string
	status          chan int
	done            chan bool
	connection      electron.Connection
	method          func(s *AMQPServer) (electron.Receiver, error)
	prefetch        int
	uniqueName      string
	reconnect       bool
	collectinterval float64
}

//NewAMQPServer   ...
func NewAMQPServer(urlStr string, debug bool, msgcount int, prefetch int, uniqueName string) *AMQPServer {
	if len(urlStr) == 0 {
		log.Println("No URL provided")
		//usage()
		os.Exit(1)
	}
	server := &AMQPServer{
		urlStr:          urlStr,
		debug:           debug,
		notifier:        make(chan string),
		status:          make(chan int),
		done:            make(chan bool),
		msgcount:        msgcount,
		method:          (*AMQPServer).connect,
		prefetch:        prefetch,
		uniqueName:      uniqueName,
		reconnect:       false,
		collectinterval: 30,
	}

	if debug {
		debugr = func(format string, data ...interface{}) {
			log.Printf(format, data...)
		}
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
	debugr("Debug: close receiver connection %s", s.connection)
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
			log.Fatalf("Could not connect to Qpid-dispatch router. is it running? : %v", err)
		}
		connectionStatus <- 1
		untilCount := s.msgcount
	theloop:
		for {
			if rm, err := r.Receive(); err == nil {
				rm.Accept()
				debugr("Message ACKed: %v", rm.Message)
				messages <- rm.Message
			} else if err == electron.Closed {
				log.Printf("Channel closed...\n")
				return
			} else {
				log.Fatalf("Received error %v: %v", s.urlStr, err)
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
		log.Println("Closed AMQP...letting loop know")
	}()

msgloop:
	for {
		select {
		case <-done:
			debugr("Done received...\n")
			break msgloop
		case m := <-messages:
			debugr("Message received... %v\n", m.Body())
			switch msg := m.Body().(type) {
			case amqp.Binary:
				s.notifier <- msg.String()
			case string:
				s.notifier <- msg
			default:
				// do nothing and report
				log.Printf("Invalid type of AMQP message received: %t", msg)
			}
		case status := <-connectionStatus:
			debugr("Status received...\n")
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
	fatalIf(err)
	c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	if err != nil {
		log.Printf("AMQP Dial tcp %v\n", err)
		return nil, err
	}

	s.connection = c // Save connection so we can Close() when start() ends

	addr := strings.TrimPrefix(url.Path, "/")
	opts := []electron.LinkOption{electron.Source(addr)}
	if s.prefetch > 0 {
		debugr("Amqp Prefetch set to %d\n", s.prefetch)
		opts = append(opts, electron.Capacity(s.prefetch), electron.Prefetch(true))
	}

	r, err := c.Receiver(opts...)
	return r, err
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
