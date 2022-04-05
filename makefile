.PHONY: local.kafka.up
local.kafka.up:
	docker compose up -d
.PHONY: local.kafka.down
local.kafka.up:
	docker compose down

# TODO: create a topic


.PHONY: ex.sarama.sub.run
ex.sarama.sub.run:
	go run examples/sarama_subscriber/main.go
.PHONY: ex.sarama.pub.run
ex.sarama.pub.run:
	go run examples/sarama_publisher/main.go

