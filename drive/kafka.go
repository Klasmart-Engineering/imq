package drive
import (
	kafka "github.com/segmentio/kafka-go"
)

func NewKafkaWriter(topic string, addr []string) *kafka.Writer{
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: addr,
		Topic:   topic,
		Balancer: &kafka.LeastBytes{},
	})
	return w
}

func NewKafkaReader(topic string, addr []string, groupId string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   addr,
		GroupID:   groupId,
		Topic:     topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	return r
}