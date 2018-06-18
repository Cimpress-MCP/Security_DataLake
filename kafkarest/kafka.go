package main

import (
	"fmt"

	"github.com/segmentio/kafka-go"
)

type KafkaWriters struct {
	writers map[string]*kafka.Writer
}

//Init - used to initialize the writer
func (k *KafkaWriters) Init() {
	k.writers = make(map[string]*kafka.Writer)

	/*
		var myConfig Configuration

		err := gonfig.GetConf("producer.json", &myConfig)
		if err != nil {
			log.Fatalf("%s", err)
			terminate(1)
		}

		log.Infof("Connecting to %s, topic %s", myConfig.Brokers, myConfig.Topic)

		myKafkaWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:  myConfig.Brokers,
			Topic:    myConfig.Topic,
			Balancer: &kafka.LeastBytes{},
		})
		defer myKafkaWriter.Close()
	*/

}

//GetWriter - Retrieve a writer to the given topic
func (k *KafkaWriters) GetWriter(channel string) (*kafka.Writer, error) {
	if k.writers[channel] != nil {
		return k.writers[channel], nil
	}

	newChannel := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  MyConfig.Brokers,
		Topic:    channel,
		Balancer: &kafka.LeastBytes{},
	})

	if newChannel == nil {
		return nil, fmt.Errorf("Error, could not connect to Kafka brokers %s on topic %s", MyConfig.Brokers, channel)
	}

	k.writers[channel] = newChannel

	return newChannel, nil
}

//CloseWriter - close the connection the given topic
func (k *KafkaWriters) CloseWriter(topic string) error {
	if k.writers[topic] != nil {
		defer k.writers[topic].Close()
		delete(k.writers, topic)
		return nil
	}

	return fmt.Errorf("Could not find open writer for topic %s", topic)
}

//CloseAll - close all existing writer connections
func (k *KafkaWriters) CloseAll() {
	for topic, connection := range k.writers {
		defer connection.Close()
		delete(k.writers, topic)
	}
}
