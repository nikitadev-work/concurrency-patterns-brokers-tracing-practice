package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func ReadMessages(ctx context.Context, done chan int, r *kafka.Reader) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Finished reading messages")
			done <- 1
			return
		default:
		}

		msg, err := r.ReadMessage(ctx)
		if err != nil {
			fmt.Printf("Failed to read message. Error: %s", err.Error())
			continue
		}

		fmt.Printf("New message read from topic %s: %s\n", msg.Topic, string(msg.Value))
	}
}

func main() {
	fmt.Println("Consumer started")

	if err := godotenv.Load(); err != nil {
		fmt.Println("Failed to load .env")
		panic(err)
	}

	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")
	groupid := "consumer-group-id2"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupID:     groupid,
		Topic:       topic,
		StartOffset: kafka.LastOffset,
	})
	defer func() {
		r.Close()
	}()

	done := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go ReadMessages(ctx, done, r)

	<-sigCh
	cancel()

	<-done
	fmt.Print("Stopped consumer")
}
