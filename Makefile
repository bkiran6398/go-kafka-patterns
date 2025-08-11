## Minimal commands to manage Kafka via docker-compose

.PHONY: kafka-up kafka-down

kafka-up:
	docker compose -f docker-compose.yml up -d

kafka-down:
	docker compose -f docker-compose.yml down

