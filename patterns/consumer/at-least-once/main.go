package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// At-Least-Once Consumer Pattern
// Ensures no message loss by committing offsets after processing
// Best for: Scenarios where message loss is unacceptable
type AtLeastOnceHandler struct{}

func (AtLeastOnceHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (AtLeastOnceHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h AtLeastOnceHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Process the message first
		log.Printf("Processing message: %s (partition: %d, offset: %d)",
			string(message.Value), message.Partition, message.Offset)

		// Simulate message processing
		err := processMessage(string(message.Value))
		if err != nil {
			log.Printf("Failed to process message: %v", err)
			// In a real scenario, you might want to retry or send to DLQ
			continue
		}

		// Only commit offset after successful processing
		session.MarkMessage(message, "")
		session.Commit()

		log.Printf("Message processed and committed: offset %d", message.Offset)
	}
	return nil
}

func processMessage(message string) error {
	// Simulate message processing logic
	// This could fail, ensuring at-least-once delivery
	log.Printf("Processing: %s", message)
	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = 10 * time.Second   // 10 seconds
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second // 3 seconds

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "at-least-once-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"at-least-once-topic"}, &AtLeastOnceHandler{}); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("At-least-once consumer started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down at-least-once consumer")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
