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

// At-Most-Once Consumer Pattern
// Commits offsets before processing to prevent duplicates
// Best for: Duplicate-sensitive scenarios where some data loss is acceptable
type AtMostOnceHandler struct{}

func (AtMostOnceHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (AtMostOnceHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h AtMostOnceHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Commit offset BEFORE processing to ensure at-most-once
		session.MarkMessage(message, "")
		session.Commit()

		log.Printf("Committed offset %d, now processing message: %s",
			message.Offset, string(message.Value))

		// Process the message after committing
		err := processMessage(string(message.Value))
		if err != nil {
			log.Printf("Failed to process message (already committed): %v", err)
			// Message is lost since offset was already committed
			continue
		}

		log.Printf("Message processed successfully: %s", string(message.Value))
	}
	return nil
}

func processMessage(message string) error {
	// Simulate message processing logic
	// If this fails, the message is lost (at-most-once guarantee)
	log.Printf("Processing: %s", message)
	return nil
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = 10 * time.Second   // 10 seconds
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second // 3 seconds

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "at-most-once-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"at-most-once-topic"}, &AtMostOnceHandler{}); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("At-most-once consumer started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down at-most-once consumer")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
