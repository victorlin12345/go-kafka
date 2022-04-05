# go-kafka

## Purpose
Comparing three famous golang kafka client libraries. And write an general pubsub interface for esay switching bewteen below libraries.

- sarama : https://github.com/Shopify/sarama
- kafka-go : https://github.com/segmentio/kafka-go
- confluent-kafka-go : https://github.com/confluentinc/confluent-kafka-go

## RoadMap

- [x] design pubsub interface
- [x] implement pubsub interface by sarama library
- [ ] implement pubsub interface by kafka-go library
- [ ] implement pubsub interface by confluent-kafka-go
- [ ] compare sarama,kafka-go,confluent-kafka-go performance

## Setup

- set up local kafka environment
```bash
make local.kafka.up
```
- create test topic
```bash
docker exec -it broker1 bash

cd opt/kafka/bin

bash kafka-topics.sh --zookeeper zookeeper:2181 --create --topic [TOPIC] --partitions [PARTITIONS] --replication-factor [REPLICAS] --if-not-exists
```

- sarama example

```bash
# subscriber
make ex.sarama.sub.run
#publisher
make ex.sarama.pub.run
```