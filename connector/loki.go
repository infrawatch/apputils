package connector

import (
    "net/http"
    "io/ioutil"
    "encoding/json"
    "bytes"
    "sync"
    "time"
    "strconv"
    "fmt"
)

type jsonValue [2]string

type jsonStream struct {
    Stream map[string]string `json:"stream"`
    Values []jsonValue `json:"values"`
}

type jsonMessage struct {
    Streams []jsonStream `json:"streams"`
}

type LokiClient struct {
    url            string
    endpoints      endpoints
    currentMessage jsonMessage
    streams        chan *jsonStream
    quit           chan struct{}
    maxBatch       int
    maxWaitTime    time.Duration
    wait           sync.WaitGroup
    batchCounter   int
    timer          *time.Timer
}

type Message struct {
    Message string
    Time    time.Duration
}

type endpoints struct {
    push  string
    query string
    ready string
}

// Checks if the loki is ready
func (client *LokiClient) IsReady() bool {
    response, err := http.Get(client.url + client.endpoints.ready)
    return err == nil && response.StatusCode == 200
}

// Creates a new loki connector
func NewLokiConnector(url string, maxBatch int, maxWaitTime time.Duration) (*LokiClient, error) {
    client := LokiClient {
        url: url,
        maxBatch: maxBatch,
        maxWaitTime: maxWaitTime,
        quit: make(chan struct{}),
        streams: make(chan *jsonStream),
        endpoints: endpoints {
            push: "/loki/api/v1/push",
            query: "/loki/api/v1/query",
            ready: "/ready",
        },
    }
    if !client.IsReady() {
        return nil, fmt.Errorf("The server on the following url is not ready: %s", url)
    }

    return &client, nil
}

// Waits for the last batch to be sent to loki
// and ends the sending goroutine created by Start()
func (client *LokiClient) Shutdown() {
    close(client.quit)
    client.wait.Wait()
}

// Starts a goroutine, which sends data to loki if
// the current batch > maxBatch or if more time
// than maxWaitTime passed
func (client *LokiClient) Start() {
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
            case stream := <-client.streams:
                client.currentMessage.Streams =
                    append(client.currentMessage.Streams, *stream)
                client.batchCounter++
                if client.batchCounter == client.maxBatch {
                    client.send()
                }
            case <-client.timer.C:
                if client.batchCounter > 0 {
                    client.send()
                } else {
                    client.timer.Reset(client.maxWaitTime)
                }
            }
        }
    }()
}

func (client *LokiClient) SendLog(labels map[string]string, message string, timestamp time.Duration) {
    m := Message {
        Message: message,
        Time: timestamp,
    }
    client.AddStream(labels, []Message{m})
}

// The template for the message sent to Loki is:
//{
//  "streams": [
//    {
//      "stream": {
//        "label": "value"
//      },
//      "values": [
//          [ "<unix epoch in nanoseconds>", "<log line>" ],
//          [ "<unix epoch in nanoseconds>", "<log line>" ]
//      ]
//    }
//  ]
//}

// Adds another stream to be sent with the next batch
func (client *LokiClient) AddStream(labels map[string]string, messages []Message) {
    var vals []jsonValue
    for i := range messages {
        var val jsonValue
        val[0] = strconv.FormatInt(int64(messages[i].Time), 10);
        val[1] = messages[i].Message
        vals = append(vals, val)
    }
    stream := jsonStream {
        Stream: labels,
        Values: vals,
    }
    client.streams <- &stream
}

// Encodes the messages and sends them to loki
func (client *LokiClient) send() error {
    str, err := json.Marshal(client.currentMessage)
    if err != nil {
        return err
    }

    response, err := http.Post(client.url + client.endpoints.push, "application/json", bytes.NewReader(str))

    client.batchCounter = 0
    client.currentMessage.Streams = []jsonStream{}
    client.timer.Reset(client.maxWaitTime)

    if response.StatusCode != 204 {
        return fmt.Errorf("Got %d http status code after pushing to loki instead of expected 204", response.StatusCode)
    } else {
        return err
    }
}

type returnedJSON struct {
    Status interface{}
    Data struct {
        ResultType string
        Result []struct {
            Stream interface{}
            Values [][]string
        }
        Stats interface{}
    }
}

// Queries the server. The queryString is expected to be in the
// LogQL format described here:
// https://github.com/grafana/loki/blob/master/docs/logql.md
func (client *LokiClient) Query(queryString string) ([]Message, error) {
    response, err := http.Get(client.url + client.endpoints.query + "?query=" + queryString)

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
                Time: time.Duration(t),
                Message: answer.Data.Result[i].Values[j][1],
            }
            values = append(values, msg)
        }
    }
    return values, nil
}

