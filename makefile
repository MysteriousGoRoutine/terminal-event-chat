COMPOSE := docker compose -f deployments/docker-compose.yaml
TOPIC := chat.messages
RUN := go run cmd/terminal-event-chat/main.go

.PHONY: run kafka-up kafka-init kafka-down kafka-topics kafka-read-topic

run:
	$(RUN)

kafka-up:
	$(COMPOSE) up -d

kafka-init: kafka-up
	@until docker exec broker /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list >/dev/null 2>&1; do \
		echo "Waiting for Kafka..."; \
		sleep 2; \
	done
	docker exec broker /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic $(TOPIC) \
		--partitions 3 \
		--replication-factor 1

kafka-topics:
	docker exec broker /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list

kafka-read-topic:
	docker exec broker /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic chat.messages \
    --from-beginning \
    # --max-messages 1

kafka-down:
	$(COMPOSE) down
