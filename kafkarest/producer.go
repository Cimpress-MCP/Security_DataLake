package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/apex/httplog"
	"github.com/apex/log"
	"github.com/gorilla/mux"
	"github.com/tkanos/gonfig"
)

// Configuration - configuration file structure
type Configuration struct {
	HTTPListenPort string
	Brokers        []string
	Topic          string
	MaxWorker      int
	MaxQueue       int
}

// JobQueue- A buffered channel that we can send work requests on.
var JobQueue chan Job

// KafaWriters - Global KafkaWriters Object
var MyKafkaWriters KafkaWriters

// MyConfig - Global Configuration Object
var MyConfig Configuration

func main() {

	err := gonfig.GetConf("producer.json", &MyConfig)
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

	MyKafkaWriters.Init()
	defer MyKafkaWriters.CloseAll()

	// crate JobQueue and Dispatcher used for writes to kafka
	JobQueue = make(chan Job)
	dispatcher := newDispatcher(MyConfig.MaxWorker)
	dispatcher.Run()

	r := mux.NewRouter()

	r.HandleFunc("/v1/syslog", http.HandlerFunc(postV1Syslog))
	r.HandleFunc("/v1/syslog/{channel}", http.HandlerFunc(postV1SyslogChannel))

	listenPort := fmt.Sprintf(":%s", MyConfig.HTTPListenPort)
	err = http.ListenAndServe(listenPort, httplog.New(r))
	if err != nil {
		log.Fatalf("%s", err)
	}

	log.Infof("Starting HTTP server listening on port %s", MyConfig.HTTPListenPort)
}

func terminate(level int) {
	os.Exit(level)
}
