package main

import (
	"fmt"

	ck "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers":                     "localhost:9092",
		"acks":                                  1,
		"max.in.flight.requests.per.connection": 1,
		"compression.type":                      "gzip",
		"reconnect.backoff.max.ms":              60000,
		"reconnect.backoff.ms":                  50,
		"log.connection.close":                  false,
	})

	if err != nil {
		fmt.Printf("Failed to construct producer: %v\n", err)
	}

	for i := 0; i < 100000; i++ {
		deliveryChan := make(chan ck.Event)
		topic := "test_topic"
		producer.Produce(&ck.Message{
			Key:   []byte("key"),
			Value: []byte(fmt.Sprintf("value %v", i)),
			TopicPartition: ck.TopicPartition{
				Topic:     &topic,
				Partition: ck.PartitionAny,
			},
		}, deliveryChan)
		r := <-deliveryChan
		switch v := r.(type) {
		case *ck.Message:
			if v.TopicPartition.Error != nil {
				fmt.Printf("Error - %v", v.TopicPartition.Error)
			}
		}
	}
}
