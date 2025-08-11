package main

import (
	"log"

	"github.com/IBM/sarama"
)

// Synchronous Send Producer Pattern
// Waits for broker acknowledgment before proceeding
// Best for: Critical data where delivery confirmation is required
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all replicas
	config.Producer.Retry.Max = 3                    // Retry up to 3 times
	config.Producer.Return.Successes = true          // Return successes

	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := &sarama.ProducerMessage{
		Topic: "synchronous-topic",
		Value: sarama.StringEncoder("Critical message"),
	}

	// Send message and wait for acknowledgment
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Printf("Message sent successfully to partition %d at offset %d", partition, offset)
}
