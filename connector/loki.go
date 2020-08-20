package connector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/infrawatch/apputils/config"
	"github.com/infrawatch/apputils/logging"
)

// The template for the message sent to Loki is:
//{
//	"streams": [
//	  {
//		"stream": {
//		  "label": "value"
//		},
//		"values": [
//			[ "<unix epoch in nanoseconds>", "<log line>" ],
//			[ "<unix epoch in nanoseconds>", "<log line>" ]
//		]
//	  }
//	]
//}

type jsonValue [2]string

//LokiStream holds one of the structure which Loki understands
type LokiStream struct {
	Stream map[string]string `json:"stream"`
	Values []jsonValue       `json:"values"`
}

//LokiLog holds one of the structure which Loki understands
type LokiLog struct {
	LogMessage string
	Timestamp  time.Duration
	Labels     map[string]string
}

type jsonMessage struct {
	Streams []LokiStream `json:"streams"`
}

//LokiConnector is the object to be used for communication with Loki
type LokiConnector struct {
	url            string
	endpoints      endpoints
	currentMessage jsonMessage
	streams        chan *LokiStream
	quit           chan struct{}
	maxBatch       int64
	maxWaitTime    time.Duration
	wait           sync.WaitGroup
	batchCounter   int64
	timer          *time.Timer
	logger         *logging.Logger
}

//Message hold structure for Loki API messages
type Message struct {
	Message string
	Time    time.Duration
}

type endpoints struct {
	push  string
	query string
	ready string
}

//IsReady checks if the loki is ready
func (client *LokiConnector) IsReady() bool {
	response, err := http.Get(client.url + client.endpoints.ready)
	return err == nil && response.StatusCode == 200
}

//ConnectLoki creates a new loki connector
func ConnectLoki(cfg config.Config, logger *logging.Logger) (*LokiConnector, error) {
	client := LokiConnector{
		quit:    make(chan struct{}),
		streams: make(chan *LokiStream),
		logger:  logger,
		endpoints: endpoints{
			push:  "/loki/api/v1/push",
			query: "/loki/api/v1/query_range",
			ready: "/ready",
		},
	}

	var err error
	var addr *config.Option
	switch conf := cfg.(type) {
	case *config.INIConfig:
		addr, err = conf.GetOption("loki/connection")
	case *config.JSONConfig:
		addr, err = conf.GetOption("Loki.Connection.Address")
	default:
		return &client, fmt.Errorf("Unknown Config type")
	}
	if err == nil && addr != nil {
		client.url = addr.GetString()
	} else {
		return nil, fmt.Errorf("Failed to get connection URL from configuration file")
	}

	var batchSize *config.Option
	switch conf := cfg.(type) {
	case *config.INIConfig:
		batchSize, err = conf.GetOption("loki/batch_size")
	case *config.JSONConfig:
		batchSize, err = conf.GetOption("Loki.Connection.BatchSize")
	}
	if err == nil && batchSize != nil {
		client.maxBatch = batchSize.GetInt()
	} else {
		return nil, fmt.Errorf("Failed to get connection max batch size from configuration file")
	}

	var waitTime *config.Option
	switch conf := cfg.(type) {
	case *config.INIConfig:
		waitTime, err = conf.GetOption("loki/max_wait_time")
	case *config.JSONConfig:
		waitTime, err = conf.GetOption("Loki.Connection.MaxWaitTime")
	}
	if err == nil && waitTime != nil {
		client.maxWaitTime = time.Duration(waitTime.GetInt()) * time.Millisecond
	} else {
		return nil, fmt.Errorf("Failed to get connection max wait time from configuration file")
	}

	err = client.Connect()
	return &client, err
}

//Connect just checks the Loki availability
func (client *LokiConnector) Connect() error {
	if !client.IsReady() {
		return fmt.Errorf("The server on the following url is not ready: %s", client.url)
	} else {
		return nil
	}
}

//Disconnect waits for the last batch to be sent to loki
// and ends the sending goroutine created by Start()
func (client *LokiConnector) Disconnect() {
	close(client.quit)
	client.wait.Wait()
}

//Start a goroutine, which sends data to loki if the current batch > maxBatch or if more time
//than maxWaitTime passed
func (client *LokiConnector) Start(outchan chan interface{}, inchan chan interface{}) {
	client.wait.Add(1)
	go func() {
		client.timer = time.NewTimer(client.maxWaitTime)

		defer func() {
			if client.batchCounter > 0 {
				client.send()
			}
			client.wait.Done()
		}()
		for {
			select {
			case <-client.quit:
				return
			case logs := <-inchan:
				switch message := logs.(type) {
				case LokiStream:
					client.addStream(message)
				case LokiLog:
					m := Message{
						Message: message.LogMessage,
						Time:    message.Timestamp,
					}
					stream := client.CreateStream(message.Labels, []Message{m})
					client.addStream(stream)
				default:
					client.logger.Metadata(map[string]interface{}{
						"logs": logs,
					})
					client.logger.Info("Skipped processing of received log stream of invalid format")
				}
			case <-client.timer.C:
				if client.batchCounter > 0 {
					client.logger.Metadata(map[string]interface{}{
						"logStreams": client.currentMessage.Streams,
					})
					client.logger.Debug("Sending logs, cause: time == maxWaitTime")
					client.send()
				} else {
					client.timer.Reset(client.maxWaitTime)
				}
			}
		}
	}()
}

func (client *LokiConnector) addStream(stream LokiStream) {
	client.currentMessage.Streams = append(client.currentMessage.Streams, stream)
	client.batchCounter++
	if client.batchCounter == client.maxBatch {
		client.logger.Metadata(map[string]interface{}{
			"logStreams": client.currentMessage.Streams,
		})
		client.logger.Debug("Sending logs, cause: batchCounter == maxBatch")
		client.send()
	}
}

//CreateStream helps to create a stream out of labels and array of messages
// each message includes timestamp and the actual log message
func (client *LokiConnector) CreateStream(labels map[string]string, messages []Message) LokiStream {
	var vals []jsonValue
	for i := range messages {
		var val jsonValue
		val[0] = strconv.FormatInt(int64(messages[i].Time), 10)
		val[1] = messages[i].Message
		vals = append(vals, val)
	}
	stream := LokiStream{
		Stream: labels,
		Values: vals,
	}
	return stream
}

// Encodes the messages and sends them to loki
func (client *LokiConnector) send() (*http.Response, error) {
	str, err := json.Marshal(client.currentMessage)
	if err != nil {
		return nil, err
	}

	response, err := http.Post(client.url+client.endpoints.push, "application/json", bytes.NewReader(str))

	client.batchCounter = 0
	client.currentMessage.Streams = []LokiStream{}
	client.timer.Reset(client.maxWaitTime)

	if err != nil {
		client.logger.Metadata(map[string]interface{}{
			"error": err,
		})
		client.logger.Error("An error occured when trying to send logs")
		return nil, err
	} else if response.StatusCode != 204 {
		client.logger.Metadata(map[string]interface{}{
			"error":    err,
			"response": response,
		})
		client.logger.Error("Recieved unexpected statuscode when trying to send logs")
		return nil, fmt.Errorf("Got %d http status code after pushing to loki instead of expected 204", response.StatusCode)
	} else {
		client.logger.Metadata(map[string]interface{}{
			"response": response,
		})
		client.logger.Debug("Logs successfuly sent")
		return response, nil
	}
}

type returnedJSON struct {
	Status interface{}
	Data   struct {
		ResultType string
		Result     []struct {
			Stream interface{}
			Values [][]string
		}
		Stats interface{}
	}
}

//Query shoud be used for uerying the server. The queryString is expected to be in the
// LogQL format described here:
// https://github.com/grafana/loki/blob/master/docs/logql.md
//
// startTime is a Unix epoch determining from which time should
// loki be looking for logs
//
// limit determines how many logs to return at most
func (client *LokiConnector) Query(queryString string, startTime time.Duration, limit int) ([]Message, error) {
	params := url.Values{}
	params.Add("query", queryString)
	params.Add("start", strconv.FormatInt(startTime.Nanoseconds(), 10))
	params.Add("limit", strconv.Itoa(limit))
	url := client.url + client.endpoints.query + "?" + params.Encode()

	client.logger.Metadata(map[string]interface{}{
		"url": url,
	})
	client.logger.Debug("Sending query to Loki")

	response, err := http.Get(url)
	if err != nil {
		return []Message{}, err
	}

	client.logger.Metadata(map[string]interface{}{
		"response": response,
	})
	client.logger.Debug("Recieved answer from loki")

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return []Message{}, err
	}
	var answer returnedJSON
	json.Unmarshal(body, &answer)
	var values []Message
	for i := range answer.Data.Result {
		for j := range answer.Data.Result[i].Values {
			t, err := strconv.Atoi(answer.Data.Result[i].Values[j][0])
			if err != nil {
				return []Message{}, err
			}
			msg := Message{
				Time:    time.Duration(t),
				Message: answer.Data.Result[i].Values[j][1],
			}
			values = append(values, msg)
		}
	}
	return values, nil
}
