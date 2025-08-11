# Go Kafka Patterns

A comprehensive collection of Kafka implementation patterns in Go, designed for learning and reference. Each pattern demonstrates a specific use case with clean, simple code that focuses on the Kafka connection and messaging patterns.

## Project Structure

```
patterns/
├── producer/
│   ├── fire-and-forget/        # High throughput, loss-tolerant
│   ├── synchronous-send/       # Critical data with delivery confirmation
│   ├── async-callback/         # Balance between performance and reliability
│   ├── batch/                  # High throughput with acceptable latency
│   └── transactional/          # Exactly-once semantics
├── consumer/
│   ├── simple/                 # Basic message consumption
│   ├── consumer-group/         # Scalable processing with load balancing
│   ├── manual-assignment/      # Custom partition assignment
│   ├── at-least-once/          # No message loss guarantee
│   └── at-most-once/           # No duplicate processing
├── stream/
│   ├── stateless-processing/   # Data transformation without state
│   └── stateful-processing/    # Aggregations with state management
├── integration/
│   └── outbox/                 # Reliable event publishing pattern
└── error-handling/
    ├── dead-letter-queue/      # Handle poison messages
    └── retry-with-backoff/     # Exponential backoff retry strategy
```

## Producer Patterns

### Fire-and-Forget (`patterns/producer/fire-and-forget/`)
- **Use Case**: High throughput scenarios where occasional message loss is acceptable
- **Configuration**: No acknowledgment required, no retries
- **Trade-offs**: Maximum performance, potential data loss

### Synchronous Send (`patterns/producer/synchronous-send/`)
- **Use Case**: Critical data where delivery confirmation is mandatory
- **Configuration**: Wait for all replicas, with retries
- **Trade-offs**: Lower throughput, guaranteed delivery

### Async with Callback (`patterns/producer/async-callback/`)
- **Use Case**: High performance with error handling capabilities
- **Configuration**: Asynchronous sending with success/error channels
- **Trade-offs**: Good performance with reliability

### Batch Producer (`patterns/producer/batch/`)
- **Use Case**: Optimize throughput by batching messages
- **Configuration**: Configurable batch size, flush frequency
- **Trade-offs**: Higher throughput, increased latency

### Transactional Producer (`patterns/producer/transactional/`)
- **Use Case**: Exactly-once delivery requirements
- **Configuration**: Idempotent producer with transaction support
- **Trade-offs**: Strong consistency, performance overhead

## Consumer Patterns

### Simple Consumer (`patterns/consumer/simple/`)
- **Use Case**: Basic message processing from specific partitions
- **Features**: Direct partition consumption
- **Best for**: Simple use cases, learning

### Consumer Group (`patterns/consumer/consumer-group/`)
- **Use Case**: Scalable message processing with automatic load balancing
- **Features**: Automatic partition assignment, rebalancing
- **Best for**: Production applications requiring scalability

### Manual Assignment (`patterns/consumer/manual-assignment/`)
- **Use Case**: Custom partition assignment logic
- **Features**: Full control over partition consumption
- **Best for**: Specialized requirements, custom load distribution

### At-Least-Once (`patterns/consumer/at-least-once/`)
- **Use Case**: Scenarios where message loss is unacceptable
- **Strategy**: Commit offsets after successful processing
- **Trade-offs**: Possible duplicates, no data loss

### At-Most-Once (`patterns/consumer/at-most-once/`)
- **Use Case**: Duplicate-sensitive processing
- **Strategy**: Commit offsets before processing
- **Trade-offs**: Potential data loss, no duplicates

## Stream Processing Patterns

### Stateless Processing (`patterns/stream/stateless-processing/`)
- **Use Case**: Data transformation, filtering, mapping
- **Features**: Transform messages without maintaining state
- **Examples**: Format conversion, data enrichment

### Stateful Processing (`patterns/stream/stateful-processing/`)
- **Use Case**: Aggregations, windowing, session tracking
- **Features**: Maintains in-memory state for computations
- **Examples**: Real-time analytics, counters, aggregations

## Integration Patterns

### Outbox Pattern (`patterns/integration/outbox/`)
- **Use Case**: Reliable event publishing with transactional safety
- **Strategy**: Write to database outbox table, then publish to Kafka
- **Benefits**: Atomic operations between database and Kafka

## Error Handling Patterns

### Dead Letter Queue (`patterns/error-handling/dead-letter-queue/`)
- **Use Case**: Handle poison messages and processing failures
- **Strategy**: Route failed messages to separate topics with retry logic
- **Features**: Configurable retry attempts, error isolation

### Retry with Backoff (`patterns/error-handling/retry-with-backoff/`)
- **Use Case**: Handle transient failures gracefully
- **Strategy**: Exponential backoff with configurable parameters
- **Features**: Prevents overwhelming downstream services

## Prerequisites

- Go 1.24.1 or later
- Kafka cluster (local or remote)
- Basic understanding of Kafka concepts

## Dependencies

```go
require (
    github.com/IBM/sarama v1.42.1
    github.com/sirupsen/logrus v1.9.3
)
```

## Running the Examples

1. **Start Kafka** (external listener on `localhost:9094`)
   - Start: `make kafka-up`
   - Stop: `make kafka-down`
   - UI: `http://localhost:8080` (cluster: "local")

2. **Create Topics** (only if auto-create is disabled)

3. **Run a Pattern**: in a pattern directory:
   ```bash
   go run main.go
   ```

## Configuration Notes

- **Broker Address**: All examples use `localhost:9094` by default
- **Topics**: Each pattern uses specific topic names (see individual files)
- **Consumer Groups**: Each pattern uses unique consumer group names
- **Error Handling**: Most patterns include basic error handling and logging

## Learning Path

1. Start with **Simple Producer** and **Simple Consumer**
2. Explore **Consumer Groups** for scalability
3. Understand delivery semantics with **At-Least-Once** and **At-Most-Once**
4. Learn stream processing with **Stateless** and **Stateful** patterns
5. Implement reliability with **Error Handling** patterns

## Key Concepts Demonstrated

- **Producer Configurations**: Different acknowledgment levels and retry strategies
- **Consumer Strategies**: Various consumption patterns and offset management
- **Error Handling**: Retry mechanisms and dead letter queues
- **Stream Processing**: Stateless and stateful message transformations
- **Integration Patterns**: Reliable event publishing strategies

## Best Practices

- Choose the right pattern based on your consistency and performance requirements
- Always handle errors appropriately for your use case
- Monitor consumer lag and processing metrics
- Test patterns with realistic message volumes
- Consider message ordering requirements when choosing patterns

## Contributing

This repository is designed for learning and reference. Each pattern is intentionally simple and focused on demonstrating the core Kafka concepts without unnecessary complexity.
