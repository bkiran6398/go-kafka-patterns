package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

// Manual Partition Assignment Pattern
// Manually assign specific partitions to consumers
// Best for: Custom partition assignment logic, full control over consumption
func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9094"}, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Manually assign specific partitions
	partitions := []int32{0, 1, 2} // Consume from partitions 0, 1, and 2
	partitionConsumers := make([]sarama.PartitionConsumer, len(partitions))

	for i, partition := range partitions {
		pc, err := consumer.ConsumePartition("manual-assignment-topic", partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("Failed to create partition consumer for partition %d: %v", partition, err)
		}
		partitionConsumers[i] = pc
		defer pc.Close()
	}

	// Handle interrupt signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	log.Printf("Manual assignment consumer started for partitions: %v", partitions)

	// Consume from all assigned partitions
	for {
		select {
		case message := <-partitionConsumers[0].Messages():
			log.Printf("Partition 0 - Message: %s (offset: %d)", string(message.Value), message.Offset)

		case message := <-partitionConsumers[1].Messages():
			log.Printf("Partition 1 - Message: %s (offset: %d)", string(message.Value), message.Offset)

		case message := <-partitionConsumers[2].Messages():
			log.Printf("Partition 2 - Message: %s (offset: %d)", string(message.Value), message.Offset)

		case err := <-partitionConsumers[0].Errors():
			log.Printf("Partition 0 error: %v", err)

		case err := <-partitionConsumers[1].Errors():
			log.Printf("Partition 1 error: %v", err)

		case err := <-partitionConsumers[2].Errors():
			log.Printf("Partition 2 error: %v", err)

		case <-signals:
			log.Println("Shutting down manual assignment consumer")
			return
		}
	}
}
