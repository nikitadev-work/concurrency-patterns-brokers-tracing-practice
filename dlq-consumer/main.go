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

func readMessages(ctx context.Context, done chan int, r *kafka.Reader) {
	var originalTopic string

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: r.Config().Brokers,
	})
	defer func() {
		w.Close()
	}()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Finished reading DLQ messages\n")
			done <- 1
			return
		default:
		}

		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Failed to read message from DLQ topic. Error: %s\n", err.Error())
			continue
		}

		fmt.Printf("Value: %s\n", string(msg.Value))
		fmt.Println("DLQ message headers:")
		for _, header := range msg.Headers {
			if header.Key == "original_topic" {
				originalTopic = string(header.Value)
			}
			fmt.Printf("  %s = %s\n", header.Key, header.Value)
		}

		// Sending message to the original topic
		replayed := true
		if originalTopic != "" {
			err := w.WriteMessages(ctx, kafka.Message{
				Value: []byte(fmt.Sprintf("\nTHIS MESSAGE WAS REPLAYED!!!\nTHIS MESSAGE WAS REPLAYED!!!\n Value: %s\n", msg.Value)),
				Headers: []kafka.Header{
					kafka.Header{Key: "replayed", Value: []byte("true")},
				},
				Topic: originalTopic,
			})
			if err != nil {
				fmt.Printf("Error while replaying the message\n")
				//replayed = false to save time now
			}
		}

		if replayed {
			err = r.CommitMessages(ctx, msg)
			if err != nil {
				fmt.Println("Failed to commit message\n")
				continue
			}
			fmt.Printf("Message committed\n")
		}
	}
}

func main() {
	fmt.Println("DLQ consumer started")

	if err := godotenv.Load(); err != nil {
		fmt.Println("Failed to load .env")
		panic(err)
	}

	broker := os.Getenv("KAFKA_BROKER")
	dlqTopik := os.Getenv("KAFKA_DLQ_TOPIC")
	groupid := "dlq-consumer-group-id"

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupID:     groupid,
		Topic:       dlqTopik,
		StartOffset: kafka.LastOffset,
	})
	defer func() {
		r.Close()
	}()

	done := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go readMessages(ctx, done, r)

	<-sigCh
	cancel()

	<-done
	fmt.Print("Stopped DLQ consumer")
}
