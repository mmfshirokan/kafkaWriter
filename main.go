package main

import (
	"context"
	"log"
	"sync"

	"strconv"

	"github.com/segmentio/kafka-go"
)

var wg sync.WaitGroup

func main() {
	broker := "localhost:9092"
	topic := "data"
	ctx := context.Background()

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go WriteDataToKafka(
			ctx,
			i*10,
			i*10+10,
			topic,
			broker,
		)
	}

	wg.Wait()
}

func WriteDataToKafka(ctx context.Context, idLowerLimit int, idUpperLimit int, topic string, broker string) {
	write := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Async:    false, // change to true for speed
		Balancer: &kafka.RoundRobin{},
	})
	defer write.Close()
	defer wg.Done()

	for i := idLowerLimit; i < idUpperLimit; i++ {
		id := []byte(strconv.FormatInt(int64(i+2000), 10)) // rm +2000
		value := []byte("pwd:" + strconv.FormatInt(int64(i), 10))
		err := write.WriteMessages(ctx, kafka.Message{
			Key:   id,
			Value: value,
		})
		if err != nil {
			log.Print("Failed to write message:", err)
		}
	}
}
