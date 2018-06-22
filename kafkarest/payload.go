package main

import (
	"context"
	"data-lake/datalakemsg"
	"encoding/json"

	"github.com/apex/log"
	"github.com/segmentio/kafka-go"
)

//Payload - a generic message container to be sent to a Kafka topic
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

	b, err := json.Marshal(&p.Message)

	err = connection.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(p.Message.ID),
			Value: b,
		},
	)

	if err != nil {
		log.Errorf("Payload:SendToKafka failed with message %s", err)
		return err
	}
	log.Infof("wrote message %s with uuid %s at %s to Kafka", string(b), p.Message.ID, p.Message.TimeStamp)

	return nil
}
