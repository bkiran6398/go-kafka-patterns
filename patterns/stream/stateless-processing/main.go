package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Stateless Stream Processing Pattern
// Transform messages without maintaining state
// Best for: Data transformation, filtering, mapping
type StatelessProcessor struct {
	producer sarama.SyncProducer
}

func (StatelessProcessor) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (StatelessProcessor) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (sp StatelessProcessor) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// Stateless transformation: convert to uppercase
		transformedValue := strings.ToUpper(string(message.Value))

		// Send transformed message to output topic
		outputMessage := &sarama.ProducerMessage{
			Topic: "transformed-topic",
			Value: sarama.StringEncoder(transformedValue),
		}

		_, _, err := sp.producer.SendMessage(outputMessage)
		if err != nil {
			log.Printf("Failed to send transformed message: %v", err)
			continue
		}

		log.Printf("Transformed: '%s' -> '%s'", string(message.Value), transformedValue)

		// Mark message as processed
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

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "stateless-processor-group", consumerConfig)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	processor := StatelessProcessor{producer: producer}

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"input-topic"}, processor); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Stateless stream processor started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down stateless processor")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
