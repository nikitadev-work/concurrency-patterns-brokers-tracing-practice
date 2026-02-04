package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func sendMessages(ctx context.Context, w *kafka.Writer, done chan int) {
	i := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Finished sending messages")
			done <- 1
			return
		default:
		}

		timestamp := time.Now()
		value := fmt.Sprintf("%d) New message from producer!", i)
		msg := []byte(value)
		err := w.WriteMessages(ctx, kafka.Message{
			Value: msg,
			Time:  timestamp,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("New message with index %d sent!\n", i)
		i++
		time.Sleep(1 * time.Second)
	}
}

func main() {
	fmt.Println("Producer started")

	if err := godotenv.Load(); err != nil {
		fmt.Println("No env file")
		panic(err)
	}

	broker := os.Getenv("KAFKA_BROKER")
	topic := os.Getenv("KAFKA_TOPIC")

	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer func() {
		w.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan int, 1)

	go sendMessages(ctx, w, done)

	<-sigCh
	cancel()

	<-done
	fmt.Println("Producer stopped")
}
