package main

import (
	"net/http"
	"os"

	"github.com/apex/log"
	"github.com/tkanos/gonfig"
)

// Configuration - configuration file structure
type Configuration struct {
	Brokers   []string
	Topic     string
	MaxWorker int
	MaxQueue  int
}

// A buffered channel that we can send work requests on.
var JobQueue chan Job

// Global KafkaWriters Object
var MyKafkaWriters KafkaWriters

// Global Configuration file object
var MyConfig Configuration

func main() {

	err := gonfig.GetConf("producer.json", &MyConfig)
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

	MyKafkaWriters.Init()
	defer MyKafkaWriters.CloseAll()

	JobQueue = make(chan Job)

	dispatcher := NewDispatcher(MyConfig.MaxWorker)
	dispatcher.Run()

	router := NewRouter()

	err = (http.ListenAndServe(":8080", router))
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

}

func terminate(level int) {
	os.Exit(level)
}

/*
func myKafkaConnection() {
	var myConfig Configuration

	err := gonfig.GetConf("producer.json", &myConfig)
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

	log.Infof("Connecting to %s, topic %s", myConfig.Brokers, myConfig.Topic)

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  myConfig.Brokers,
		Topic:    myConfig.Topic,
		Balancer: &kafka.LeastBytes{},
	})

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Test"),
			Value: []byte("Value"),
		},
	)

	if err != nil {
		log.Errorf("%s", err)
	}
}
*/
