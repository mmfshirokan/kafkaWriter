package main

import (
	"context"
	"strconv"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := "localhost:1011"
	topic := "topic"
	ctx := context.Background()

	write := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})

	for i := 0; i < 2000; i++ {
		write.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.FormatInt(int64(i), 10)),
			Value: []byte("abcd"),
		})
	}

}
