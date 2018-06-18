package main

import (
	"bytes"
	"context"
	"data-lake/datalakemsg"
	"encoding/json"
	"net/http"
	"time"

	"github.com/apex/log"
	metrics "github.com/rcrowley/go-metrics"
)

// RestConnection - http connection with metrics
type RestConnection struct {
	fpURL string

	registry metrics.Registry
	msgTx    metrics.Counter
	bytesTx  metrics.Counter
}

// NewRestConnection - returns a connection object to be used for later write calls
func NewRestConnection(httpURL string) (*RestConnection, error) {
	connect := &RestConnection{
		fpURL:    httpURL,
		registry: metrics.NewRegistry(),
		msgTx:    metrics.NewCounter(),
		bytesTx:  metrics.NewCounter(),
	}

	connect.registry.Register("messages.transmitted", connect.msgTx)
	connect.registry.Register("message.bytes.transferred", connect.bytesTx)

	return connect, nil
}

//Write - writes the given string as HTTP POST to the RestConnection
func (r *RestConnection) Write(s string) {
	msg := datalakemsg.NewSyslogV1()
	msg.Body = s

	body, _ := json.Marshal(msg)
	req, err := http.NewRequest("POST", r.fpURL, bytes.NewBuffer(body))
	if err != nil {
		log.Fatalf("%v", err)
	}

	ctx, cancel := context.WithTimeout(req.Context(), 3*time.Second)
	defer cancel()

	req = req.WithContext(ctx)

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		log.Infof("%v", err)
		return
	}

	r.msgTx.Inc(1)
	r.bytesTx.Inc(int64(len(s)))
}

//Statistics - return current statistics of this connection
func (r *RestConnection) Statistics() (metrics.Registry, error) {
	return r.registry, nil
}
