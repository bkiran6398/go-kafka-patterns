package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

// Batch Producer Pattern
// Accumulates messages before sending in batches
// Best for: High throughput scenarios with acceptable latency
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 500 * time.Millisecond // flush every 500ms
	config.Producer.Flush.Messages = 100                     // target 100 messages per batch
	config.Producer.Flush.Bytes = 1 * 1024 * 1024            // or 1MB
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create async producer: %v", err)
	}

	var successes int64
	var failures int64
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range producer.Successes() {
			atomic.AddInt64(&successes, 1)
		}
	}()

	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Printf("Send error: %v", err)
			atomic.AddInt64(&failures, 1)
		}
	}()

	numMessages := 200
	for range numMessages {
		producer.Input() <- &sarama.ProducerMessage{
			Topic: "batch-topic",
			Value: sarama.StringEncoder("Batch message"),
		}
	}

	// Signal we are done producing; this will flush remaining batches and close channels
	producer.AsyncClose()
	wg.Wait()

	log.Printf("Batch producer finished: successes=%d, errors=%d", successes, failures)
}
