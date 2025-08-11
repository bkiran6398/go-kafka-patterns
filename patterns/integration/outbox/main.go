package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

// Outbox Pattern
// Ensures reliable event publishing by writing to database outbox first
// Best for: Transactional safety between database and Kafka operations

type OutboxEvent struct {
	ID        string    `json:"id"`
	EventType string    `json:"event_type"`
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}

type OutboxPublisher struct {
	producer sarama.SyncProducer
}

func NewOutboxPublisher() (*OutboxPublisher, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		return nil, err
	}

	return &OutboxPublisher{producer: producer}, nil
}

func (op *OutboxPublisher) Close() error {
	return op.producer.Close()
}

// Simulate saving to database outbox table
func (op *OutboxPublisher) saveToOutbox(event OutboxEvent) error {
	log.Printf("Saving to outbox table: %+v", event)
	// In real implementation, this would be a database transaction
	// that saves both business data and outbox event atomically
	return nil
}

// Process outbox events and publish to Kafka
func (op *OutboxPublisher) processOutboxEvents(events []OutboxEvent) error {
	for _, event := range events {
		// Publish event to Kafka
		eventData, err := json.Marshal(event)
		if err != nil {
			log.Printf("Failed to marshal event %s: %v", event.ID, err)
			continue
		}

		message := &sarama.ProducerMessage{
			Topic: "outbox-events",
			Key:   sarama.StringEncoder(event.ID),
			Value: sarama.ByteEncoder(eventData),
		}

		partition, offset, err := op.producer.SendMessage(message)
		if err != nil {
			log.Printf("Failed to publish event %s: %v", event.ID, err)
			// In real implementation, you would retry or handle the error appropriately
			continue
		}

		log.Printf("Published event %s to partition %d at offset %d", event.ID, partition, offset)

		// Mark event as processed in outbox table
		op.markEventAsProcessed(event.ID)
	}

	return nil
}

func (op *OutboxPublisher) markEventAsProcessed(eventID string) {
	log.Printf("Marking event %s as processed in outbox table", eventID)
	// In real implementation, this would update the database record
}

// Simulate polling outbox table for unprocessed events
func (op *OutboxPublisher) pollOutboxEvents() []OutboxEvent {
	// In real implementation, this would query the database for unprocessed events
	return []OutboxEvent{
		{
			ID:        "event-1",
			EventType: "user_created",
			Payload:   `{"user_id": "123", "email": "user@example.com"}`,
			CreatedAt: time.Now(),
		},
		{
			ID:        "event-2",
			EventType: "order_placed",
			Payload:   `{"order_id": "456", "user_id": "123", "amount": 99.99}`,
			CreatedAt: time.Now(),
		},
	}
}

func main() {
	publisher, err := NewOutboxPublisher()
	if err != nil {
		log.Fatalf("Failed to create outbox publisher: %v", err)
	}
	defer publisher.Close()

	// Simulate business operation that creates outbox events
	businessEvents := []OutboxEvent{
		{
			ID:        "biz-event-1",
			EventType: "payment_processed",
			Payload:   `{"payment_id": "789", "amount": 150.00}`,
			CreatedAt: time.Now(),
		},
	}

	// Step 1: Save business data and outbox events in same transaction
	for _, event := range businessEvents {
		if err := publisher.saveToOutbox(event); err != nil {
			log.Printf("Failed to save to outbox: %v", err)
			continue
		}
	}

	// Step 2: Poll outbox table and publish events to Kafka
	// This would typically run in a separate process/goroutine
	unprocessedEvents := publisher.pollOutboxEvents()
	if err := publisher.processOutboxEvents(unprocessedEvents); err != nil {
		log.Printf("Failed to process outbox events: %v", err)
	}

	log.Println("Outbox pattern example completed")
}
