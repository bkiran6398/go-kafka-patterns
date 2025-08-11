package main

import (
	"context"
	"log"
	"math"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Retry with Exponential Backoff Pattern
// Retries failed operations with increasing delays
// Best for: Handling transient failures

type RetryHandler struct {
	maxRetries    int
	baseDelay     time.Duration
	maxDelay      time.Duration
	backoffFactor float64
}

func NewRetryHandler(maxRetries int, baseDelay, maxDelay time.Duration, backoffFactor float64) *RetryHandler {
	return &RetryHandler{
		maxRetries:    maxRetries,
		baseDelay:     baseDelay,
		maxDelay:      maxDelay,
		backoffFactor: backoffFactor,
	}
}

func (RetryHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (RetryHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *RetryHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Processing message: %s", string(message.Value))

		// Attempt to process message with retry logic
		err := h.processWithRetry(string(message.Value))
		if err != nil {
			log.Printf("Failed to process message after %d retries: %v", h.maxRetries, err)
			// Could send to DLQ here
		} else {
			log.Printf("Message processed successfully: %s", string(message.Value))
		}

		session.MarkMessage(message, "")
	}
	return nil
}

func (h *RetryHandler) processWithRetry(message string) error {
	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		err := h.processMessage(message)
		if err == nil {
			if attempt > 0 {
				log.Printf("Message processed successfully on attempt %d", attempt+1)
			}
			return nil
		}

		if attempt < h.maxRetries {
			delay := h.calculateBackoff(attempt)
			log.Printf("Attempt %d failed: %v. Retrying in %v", attempt+1, err, delay)
			time.Sleep(delay)
		}
	}

	return &MaxRetriesExceededError{
		Attempts:  h.maxRetries + 1,
		LastError: h.processMessage(message), // Get the final error
	}
}

func (h *RetryHandler) calculateBackoff(attempt int) time.Duration {
	// Exponential backoff: baseDelay * (backoffFactor ^ attempt)
	delay := time.Duration(float64(h.baseDelay) * math.Pow(h.backoffFactor, float64(attempt)))

	// Cap at maxDelay
	if delay > h.maxDelay {
		delay = h.maxDelay
	}

	return delay
}

func (h *RetryHandler) processMessage(message string) error {
	// Simulate processing that might fail transiently
	switch message {
	case "temporary-failure":
		return &TransientError{Message: "Temporary service unavailable"}
	case "network-error":
		return &NetworkError{Message: "Network connection failed"}
	case "permanent-failure":
		return &PermanentError{Message: "Invalid data format"}
	default:
		// Simulate successful processing
		return nil
	}
}

// Error types
type TransientError struct {
	Message string
}

func (e *TransientError) Error() string {
	return "Transient error: " + e.Message
}

type NetworkError struct {
	Message string
}

func (e *NetworkError) Error() string {
	return "Network error: " + e.Message
}

type PermanentError struct {
	Message string
}

func (e *PermanentError) Error() string {
	return "Permanent error: " + e.Message
}

type MaxRetriesExceededError struct {
	Attempts  int
	LastError error
}

func (e *MaxRetriesExceededError) Error() string {
	return "Max retries exceeded after " + string(rune(e.Attempts)) + " attempts. Last error: " + e.LastError.Error()
}

func main() {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9094"}, "retry-handler-group", config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Configure retry handler: max 3 retries, base delay 100ms, max delay 5s, 2x backoff
	handler := NewRetryHandler(3, 100*time.Millisecond, 5*time.Second, 2.0)

	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{"retry-topic"}, handler); err != nil {
				log.Printf("Error consuming: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	log.Println("Retry handler with exponential backoff started")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	log.Println("Shutting down retry handler")
	cancel()
	wg.Wait()

	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
}
