package main

import (
	"data-lake/collector/input"
	"data-lake/collector/rest"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/apex/log"
	"github.com/rcrowley/go-metrics"
	"github.com/tkanos/gonfig"
)

//application resources
var tcpServer *input.TcpServer
var udpServer *input.UdpServer
var parser *input.Rfc5424Parser
var producer *rest.RestConnection

//Diagnostic data
var startTime time.Time

const (
	connTcpHost  = "localhost:514"
	connUdpHost  = "localhost:514"
	parseEnabled = true
	cCapacity    = 0 // capacity of channel
)

var myConfig ConfigFile

type ConfigFile struct {
	ConnTcpPort string
	ConnUdpPort string
	RestURL     string
	AdminPort   string
}

type Stats interface {
	Statistics() (metrics.Registry, error)
}

// return server statistics for admin interface
func servStatistics(w http.ResponseWriter, r *http.Request) {
	stats := make(map[string]interface{})
	resources := map[string]Stats{"tcp": tcpServer, "udp": udpServer, "rest": producer}
	for k, v := range resources {
		if v == nil { // service is not enabled, skip
			continue
		}

		s, err := v.Statistics()
		if err != nil {
			log.Errorf("failed to get statistics for %s", k)
			http.Error(w, "failed to get statistics for "+k, http.StatusInternalServerError)
			return
		}
		stats[k] = s
	}

	b, err := json.MarshalIndent(stats, "", "   ")
	if err != nil {
		log.Error("Failed to JSON marshall statistics map")
		http.Error(w, "failed to JSON marshall statistics map", http.StatusInternalServerError)
		return
	}
	w.Write(b)
}

func servHealthCheck(w http.ResponseWriter, r *http.Request) {
	if producer.IsTransmitting == true {
		return
	}

	s, err := producer.Statistics()

	if s == nil || err != nil {
		log.Error("Failed to receive statistics from Rest Producer for healthcheck")
		http.Error(w, "failed to receive statistics from Rest producer", http.StatusInternalServerError)
		return
	}

	i := s.Get("messages.errors")
	if i == nil {
		log.Error("Failed to receive messages.errors from Rest Producer for healthcheck")
		http.Error(w, "failed to receive messages.errors from Rest producer", http.StatusInternalServerError)
		return
	}

	switch metric := i.(type) {
	case metrics.Counter:
		errCount := metric.Count()

		if errCount != 0 {
			log.Errorf("Rest connection is failing, count is %d", errCount)
			http.Error(w, "Rest connection backend is failing", http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprintf(w, "Health check passed successfully.")
}

func main() {
	var err error

	startTime = time.Now()

	err = gonfig.GetConf("collector.json", &myConfig)
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

	//make channels for threaded communication
	rawChan := make(chan string, cCapacity)
	prodChan := make(chan string, cCapacity)

	parser = input.NewRfc5424Parser()

	// send the raw input channel through the parser.
	// JSON parsed output is to prodChan
	prodChan, err = parser.StreamingParse(rawChan)

	// start tcp interface and hook it to the raw input channel
	tcpServer = input.NewTcpServer(myConfig.ConnTcpPort)
	err = tcpServer.Start(func() chan<- string {
		return rawChan
	})
	if err != nil {
		log.Errorf("Failed to start TCP server.  error: %s", err)
		os.Exit(1)
	}
	log.Infof("listening to TCP connections on port %s", myConfig.ConnTcpPort)

	// start UDP server
	udpServer = input.NewUdpServer(myConfig.ConnUdpPort)
	err = udpServer.Start(func() chan<- string {
		return rawChan
	})
	if err != nil {
		log.Errorf("Failed to start UDP server.  error: %s", err)
		os.Exit(1)
	}
	log.Infof("listening to UDP connections on port %s", myConfig.ConnUdpPort)

	// start up statistics and healthcheck http service
	http.HandleFunc("/statistics", servStatistics)
	http.HandleFunc("/healthcheck", servHealthCheck)

	// spawn thread to handle stats http requests
	go func() {
		err := http.ListenAndServe(myConfig.AdminPort, nil)
		if err != nil {
			log.Fatalf("Error, could not start admin interface, error : %s", err)
		}
	}()
	log.Infof("Admin interface started on %s", myConfig.AdminPort)

	producer, err = rest.NewRestConnection(myConfig.RestURL)

	for { // send anything that comes in from the incoming producer channel to our REST producer
		producer.Write(<-prodChan)
	}
}

func terminate(level int) {
	os.Exit(level)
}
