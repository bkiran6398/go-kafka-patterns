package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

// Simple Consumer Pattern
// Basic message consumption from a topic
// Best for: Simple use cases, single consumer scenarios
func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Consume from partition 0
	partitionConsumer, err := consumer.ConsumePartition("simple-topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create partition consumer: %v", err)
	}
	defer partitionConsumer.Close()

	// Handle interrupt signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Println("Simple consumer started")

	for {
		select {
		case message := <-partitionConsumer.Messages():
			log.Printf("Received message: %s (offset: %d)", string(message.Value), message.Offset)

		case err := <-partitionConsumer.Errors():
			log.Printf("Consumer error: %v", err)

		case <-signals:
			log.Println("Shutting down simple consumer")
			return
		}
	}
}
