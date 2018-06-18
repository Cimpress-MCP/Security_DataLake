package main

import (
	"context"
	"data-lake/datalakemsg"

	"github.com/apex/log"
	"github.com/segmentio/kafka-go"
)

type Payload struct {
	Message datalakemsg.LogMessage
	Topic   string
}

//SendToKafka - send this message to its Kafka topic
func (p *Payload) SendToKafka() error {
	connection, err := MyKafkaWriters.GetWriter(p.Topic)
	if err != nil {
		return err
	}

	marshalledMsg, err := p.Message.GetJSON()

	err = connection.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(p.Message.ID),
			Value: []byte(marshalledMsg),
		},
	)

	if err != nil {
		return err
	}
	log.Debugf("wrote message %s with uuid %s at %s to Kafka", marshalledMsg, p.Message.ID, p.Message.TimeStamp)

	return nil
}
