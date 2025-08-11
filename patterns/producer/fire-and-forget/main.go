package main

import (
	"log"

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

	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: "fire-and-forget-topic",
		Value: sarama.StringEncoder("Hello, Kafka!"),
	}

	// Send message without waiting for response
	_, _, err = producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message: %v", err)
	}

	log.Println("Message sent (fire-and-forget)")
}
