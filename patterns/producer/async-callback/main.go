package main

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

// Asynchronous Producer with Callback Pattern
// Sends messages asynchronously and handles success/failure via channels
// Best for: Balance between performance and reliability
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal // Use WaitForLocal instead of undefined WaitForLeader
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Handle success and error callbacks
	go func() {
		for success := range producer.Successes() {
			log.Printf("Message sent successfully to partition %d at offset %d",
				success.Partition, success.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Failed to send message: %v", err.Err)
		}
	}()

	// Send messages asynchronously
	message := &sarama.ProducerMessage{
		Topic: "async-topic",
		Value: sarama.StringEncoder("Async message"),
	}

	producer.Input() <- message

	// Give time for callback processing
	time.Sleep(2 * time.Second)
	log.Println("Async producer finished")
}
