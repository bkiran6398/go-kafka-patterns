package main

import (
	"log"

	"github.com/IBM/sarama"
)

// Batch Producer Pattern
// Accumulates messages before sending in batches
// Best for: High throughput scenarios with acceptable latency
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 500 * 1000 * 1000 // 500ms
	config.Producer.Flush.Messages = 100                // Batch size of 100 messages
	config.Producer.Flush.Bytes = 1024 * 1024           // 1MB batch size
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Send multiple messages - they will be batched automatically
	for i := range 10 {
		message := &sarama.ProducerMessage{
			Topic: "batch-topic",
			Value: sarama.StringEncoder("Batch message"),
		}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Printf("Failed to send message %d: %v", i, err)
		} else {
			log.Printf("Message %d sent to partition %d at offset %d", i, partition, offset)
		}
	}

	log.Println("Batch producer finished")
}
