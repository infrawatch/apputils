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
	Url            string
	Endpoints      Endpoints
	CurrentMessage jsonMessage
	Streams        chan *LokiStream
	Quit           chan struct{}
	MaxBatch       int64
	MaxWaitTime    time.Duration
	Wait           sync.WaitGroup
	BatchCounter   int64
	Timer          *time.Timer
	Logger         *logging.Logger
}

//Message hold structure for Loki API messages
type Message struct {
	Message string
	Time    time.Duration
}

type Endpoints struct {
	Push  string
	Query string
	Ready string
}

//IsReady checks if the loki is ready
func (client *LokiConnector) IsReady() bool {
	response, err := http.Get(client.Url + client.Endpoints.Ready)
	return err == nil && response.StatusCode == 200
}

//ConnectLoki creates a new loki connector
func ConnectLoki(cfg config.Config, logger *logging.Logger) (*LokiConnector, error) {
	client := LokiConnector{
		Quit:    make(chan struct{}),
		Streams: make(chan *LokiStream),
		Logger:  logger,
		Endpoints: Endpoints{
			Push:  "/loki/api/v1/push",
			Query: "/loki/api/v1/query_range",
			Ready: "/ready",
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
		client.Url = addr.GetString()
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
		client.MaxBatch = batchSize.GetInt()
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
		client.MaxWaitTime = time.Duration(waitTime.GetInt()) * time.Millisecond
	} else {
		return nil, fmt.Errorf("Failed to get connection max wait time from configuration file")
	}

	err = client.Connect()
	return &client, err
}

//Connect just checks the Loki availability
func (client *LokiConnector) Connect() error {
	if !client.IsReady() {
		return fmt.Errorf("The server on the following url is not ready: %s", client.Url)
	} else {
		return nil
	}
}

//Disconnect waits for the last batch to be sent to loki
// and ends the sending goroutine created by Start()
func (client *LokiConnector) Disconnect() {
	close(client.Quit)
	client.Wait.Wait()
}

//Start a goroutine, which sends data to loki if the current batch > maxBatch or if more time
//than maxWaitTime passed
func (client *LokiConnector) Start(outchan chan interface{}, inchan chan interface{}) {
	client.Wait.Add(1)
	go func() {
		client.Timer = time.NewTimer(client.MaxWaitTime)

		defer func() {
			if client.BatchCounter > 0 {
				client.send()
			}
			client.Wait.Done()
		}()
		for {
			select {
			case <-client.Quit:
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
					client.Logger.Metadata(map[string]interface{}{
						"logs": logs,
					})
					client.Logger.Info("Skipped processing of received log stream of invalid format")
				}
			case <-client.Timer.C:
				if client.BatchCounter > 0 {
					client.Logger.Metadata(map[string]interface{}{
						"logStreams": client.CurrentMessage.Streams,
					})
					client.Logger.Debug("Sending logs, cause: time == maxWaitTime")
					client.send()
				} else {
					client.Timer.Reset(client.MaxWaitTime)
				}
			}
		}
	}()
}

func (client *LokiConnector) addStream(stream LokiStream) {
	client.CurrentMessage.Streams = append(client.CurrentMessage.Streams, stream)
	client.BatchCounter++
	if client.BatchCounter == client.MaxBatch {
		client.Logger.Metadata(map[string]interface{}{
			"logStreams": client.CurrentMessage.Streams,
		})
		client.Logger.Debug("Sending logs, cause: batchCounter == maxBatch")
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
	str, err := json.Marshal(client.CurrentMessage)
	if err != nil {
		return nil, err
	}

	response, err := http.Post(client.Url+client.Endpoints.Push, "application/json", bytes.NewReader(str))

	client.BatchCounter = 0
	client.CurrentMessage.Streams = []LokiStream{}
	client.Timer.Reset(client.MaxWaitTime)

	if err != nil {
		client.Logger.Metadata(map[string]interface{}{
			"error": err,
		})
		client.Logger.Error("An error occured when trying to send logs")
		return nil, err
	} else if response.StatusCode != 204 {
		client.Logger.Metadata(map[string]interface{}{
			"error":    err,
			"response": response,
		})
		client.Logger.Error("Recieved unexpected statuscode when trying to send logs")
		return nil, fmt.Errorf("Got %d http status code after pushing to loki instead of expected 204", response.StatusCode)
	} else {
		client.Logger.Metadata(map[string]interface{}{
			"response": response,
		})
		client.Logger.Debug("Logs successfuly sent")
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
	url := client.Url + client.Endpoints.Query + "?" + params.Encode()

	client.Logger.Metadata(map[string]interface{}{
		"url": url,
	})
	client.Logger.Debug("Sending query to Loki")

	response, err := http.Get(url)
	if err != nil {
		return []Message{}, err
	}

	client.Logger.Metadata(map[string]interface{}{
		"response": response,
	})
	client.Logger.Debug("Recieved answer from loki")

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
