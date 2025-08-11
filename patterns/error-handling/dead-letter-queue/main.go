package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Dead Letter Queue Pattern
// Routes failed messages to a separate topic for later processing
// Best for: Handling poison messages and processing failures

type DLQHandler struct {
	producer   sarama.SyncProducer
	maxRetries int
}

func NewDLQHandler(producer sarama.SyncProducer, maxRetries int) *DLQHandler {
	return &DLQHandler{
		producer:   producer,
		maxRetries: maxRetries,
	}
}

func (DLQHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (DLQHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *DLQHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Processing message: %s", string(message.Value))

		// Attempt to process message
		err := h.processMessage(string(message.Value))
		if err != nil {
			log.Printf("Failed to process message: %v", err)

			// Get retry count from headers (simulated)
			retryCount := h.getRetryCount(message)

			if retryCount < h.maxRetries {
				// Send to retry topic with incremented retry count
				h.sendToRetryTopic(message, retryCount+1)
				log.Printf("Message sent to retry topic (attempt %d/%d)", retryCount+1, h.maxRetries)
			} else {
				// Send to dead letter queue
				h.sendToDeadLetterQueue(message, err)
				log.Printf("Message sent to dead letter queue after %d attempts", retryCount)
			}
		} else {
			log.Printf("Message processed successfully: %s", string(message.Value))
		}

		// Always mark message as processed to avoid reprocessing
		session.MarkMessage(message, "")
	}
	return nil
}

func (h *DLQHandler) processMessage(message string) error {
	// Simulate message processing that might fail
	if message == "poison-message" {
		return &ProcessingError{Message: "Poison message detected"}
	}
	if message == "temporary-failure" {
		return &TemporaryError{Message: "Temporary processing failure"}
	}
	return nil
}

func (h *DLQHandler) getRetryCount(message *sarama.ConsumerMessage) int {
	// In real implementation, extract retry count from message headers
	if retryCountHeader := h.getHeader(message, "retry-count"); retryCountHeader != nil {
		// Parse retry count from header
		return 0 // Simplified
	}
	return 0
}

func (h *DLQHandler) getHeader(message *sarama.ConsumerMessage, key string) []byte {
	for _, header := range message.Headers {
		if string(header.Key) == key {
			return header.Value
		}
	}
	return nil
}

func (h *DLQHandler) sendToRetryTopic(originalMessage *sarama.ConsumerMessage, retryCount int) {
	retryMessage := &sarama.ProducerMessage{
		Topic: "retry-topic",
		Value: sarama.ByteEncoder(originalMessage.Value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("retry-count"), Value: []byte{byte(retryCount)}},
			{Key: []byte("original-topic"), Value: []byte(originalMessage.Topic)},
		},
	}

	_, _, err := h.producer.SendMessage(retryMessage)
	if err != nil {
		log.Printf("Failed to send message to retry topic: %v", err)
	}
}

func (h *DLQHandler) sendToDeadLetterQueue(originalMessage *sarama.ConsumerMessage, processingError error) {
	dlqMessage := &sarama.ProducerMessage{
		Topic: "dead-letter-queue",
		Value: sarama.ByteEncoder(originalMessage.Value),
		Headers: []sarama.RecordHeader{
			{Key: []byte("error"), Value: []byte(processingError.Error())},
			{Key: []byte("original-topic"), Value: []byte(originalMessage.Topic)},
			{Key: []byte("original-partition"), Value: []byte{byte(originalMessage.Partition)}},
		},
	}

	_, _, err := h.producer.SendMessage(dlqMessage)
	if err != nil {
		log.Printf("Failed to send message to dead letter queue: %v", err)
	}
}

type ProcessingError struct {
	Message string
}

func (e *ProcessingError) Error() string {
	return e.Message
}

type TemporaryError struct {
	Message string
}

func (e *TemporaryError) Error() string {
	return e.Message
}

func main() {
	// Producer configuration
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
	producerConfig.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9094"}, producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Consumer configuration
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "dlq-handler-group", consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	handler := NewDLQHandler(producer, 3) // Max 3 retries

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"main-topic"}, handler); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Dead Letter Queue handler started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down DLQ handler")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
