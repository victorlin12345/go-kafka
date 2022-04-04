.PHONY: sarama.sub.run
sarama.sub.run:
	go run examples/sarama_subscriber/main.go
.PHONY: sarama.pub.run
sarama.pub.run:
	go run examples/sarama_publisher/main.go
