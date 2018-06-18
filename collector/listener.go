package main

import (
	"os"
	"time"

	"github.com/apex/log"
	"github.com/tkanos/gonfig"
)

//application resources
var tcpServer *TcpServer
var udpServer *UdpServer
var parser *Rfc5424Parser

//Diagnostic data
var startTime time.Time

const (
	connTcpHost  = "localhost:514"
	connUdpHost  = "localhost:514"
	parseEnabled = true
	cCapacity    = 0 // capacity of channel
)

type ConfigFile struct {
	ConnTcpPort string
	ConnUdpPort string
	RestURL     string
}

func main() {
	var err error
	var myConfig ConfigFile

	startTime = time.Now()

	err = gonfig.GetConf("collector.json", &myConfig)
	if err != nil {
		log.Fatalf("%s", err)
		terminate(1)
	}

	//make channels for threaded communication
	rawChan := make(chan string, cCapacity)
	prodChan := make(chan string, cCapacity)

	parser = NewRfc5424Parser()

	// send the raw input channel through the parser.
	// JSON parsed output is to prodChan
	prodChan, err = parser.StreamingParse(rawChan)

	// start tcp interface and hook it to the raw input channel
	tcpServer = NewTcpServer(myConfig.ConnTcpPort)
	err = tcpServer.Start(func() chan<- string {
		return rawChan
	})
	if err != nil {
		log.Errorf("Failed to start TCP server.  error: %s", err)
		os.Exit(1)
	}
	log.Infof("listening to TCP connections on port %s", myConfig.ConnTcpPort)

	// start UDP server
	udpServer = NewUdpServer(myConfig.ConnUdpPort)
	err = udpServer.Start(func() chan<- string {
		return rawChan
	})
	if err != nil {
		log.Errorf("Failed to start UDP server.  error: %s", err)
		os.Exit(1)
	}
	log.Infof("listening to UDP connections on port %s", myConfig.ConnUdpPort)

	producer, err := NewRestConnection(myConfig.RestURL)

	for {
		producer.Write(<-prodChan)
	}
}

func terminate(level int) {
	os.Exit(level)
}
