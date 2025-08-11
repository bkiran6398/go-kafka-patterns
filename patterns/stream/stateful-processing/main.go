package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Stateful Stream Processing Pattern
// Maintains state for aggregations, windowing, etc.
// Best for: Real-time analytics, aggregations, session tracking

type EventData struct {
	UserID string `json:"user_id"`
	Action string `json:"action"`
	Count  int    `json:"count"`
}

type StatefulProcessor struct {
	producer sarama.SyncProducer
	state    map[string]int // In-memory state store (user_id -> count)
	mu       *sync.RWMutex
}

func NewStatefulProcessor(producer sarama.SyncProducer) *StatefulProcessor {
	return &StatefulProcessor{
		producer: producer,
		state:    make(map[string]int),
	}
}

func (StatefulProcessor) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (StatefulProcessor) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (sp *StatefulProcessor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var event EventData
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Failed to unmarshal event: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		// Update state (stateful operation)
		sp.mu.Lock()
		sp.state[event.UserID] += event.Count
		currentCount := sp.state[event.UserID]
		sp.mu.Unlock()

		log.Printf("User %s: %s (total count: %d)", event.UserID, event.Action, currentCount)

		// Send aggregated result if threshold is reached
		if currentCount >= 10 {
			aggregatedEvent := EventData{
				UserID: event.UserID,
				Action: "threshold_reached",
				Count:  currentCount,
			}

			aggregatedData, _ := json.Marshal(aggregatedEvent)
			outputMessage := &sarama.ProducerMessage{
				Topic: "aggregated-topic",
				Value: sarama.ByteEncoder(aggregatedData),
			}

			_, _, err := sp.producer.SendMessage(outputMessage)
			if err != nil {
				log.Printf("Failed to send aggregated message: %v", err)
			} else {
				log.Printf("Sent aggregated event for user %s (count: %d)", event.UserID, currentCount)
			}

			// Reset counter for this user
			sp.mu.Lock()
			sp.state[event.UserID] = 0
			sp.mu.Unlock()
		}

		session.MarkMessage(message, "")
	}
	return nil
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

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "stateful-processor-group", consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	processor := NewStatefulProcessor(producer)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"events-topic"}, processor); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Stateful stream processor started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down stateful processor")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
