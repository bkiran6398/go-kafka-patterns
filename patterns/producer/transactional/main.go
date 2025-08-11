package main

import (
	"log"

	"github.com/IBM/sarama"
)

// Transactional Producer Pattern
// Uses Kafka transactions for exactly-once semantics
// Best for: Financial transactions, exactly-once delivery requirements
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Idempotent = true
	config.Producer.Transaction.ID = "my-transaction-id"
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Net.MaxOpenRequests = 1

	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Begin a transaction before sending
	if err := producer.BeginTxn(); err != nil {
		log.Fatalf("BeginTxn failed: %v", err)
	}

	// Note: Full transactional support requires Kafka 0.11+
	message := &sarama.ProducerMessage{
		Topic: "transactional-topic",
		Value: sarama.StringEncoder("Transactional message"),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		// Abort the transaction on error
		_ = producer.AbortTxn()
		log.Fatalf("Failed to send transactional message: %v", err)
	}

	if err := producer.CommitTxn(); err != nil {
		log.Fatalf("CommitTxn failed: %v", err)
	}

	log.Printf("Transactional message sent to partition %d at offset %d", partition, offset)
}
