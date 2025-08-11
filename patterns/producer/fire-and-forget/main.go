package main

import (
	"log"
	"time"

	"github.com/IBM/sarama"
)

// Fire-and-Forget Producer Pattern
// Sends messages without waiting for acknowledgment
// Best for: High throughput, loss-tolerant scenarios
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.NoResponse // Don't wait for any acknowledgment
	config.Producer.Retry.Max = 0                    // No retries
	config.Producer.Return.Successes = false         // Don't return successes
	config.Producer.Return.Errors = false            // Don't require reading errors channel

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: "fire-and-forget-topic",
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	// Send message without waiting for response
	producer.Input() <- message

	// Brief sleep to hand off to producer goroutine (fire-and-forget)
	time.Sleep(300 * time.Millisecond)
	log.Println("Message dispatched (fire-and-forget)")
}
