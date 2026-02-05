package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func readMessages(ctx context.Context, done chan int, r *kafka.Reader, dlqWriter *kafka.Writer, baseDelay time.Duration) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Finished reading messages")
			done <- 1
			return
		default:
		}

		msg, err := r.FetchMessage(ctx)
		if err != nil {
			fmt.Printf("Failed to read message. Error: %s\n", err.Error())
			continue
		}

		v := rand.Float32()
		processed := false
		dlqed := false
		for _, h := range msg.Headers {
			if h.Key == "replayed" && string(h.Value) == "true" {
				v = 1
			}
		}
		if v <= 0.8 {
			fmt.Println("ERROR!!!")

			// retry for 5 times
			for i := 1; i <= 5; i++ {
				// backoff + jitter
				delay := baseDelay << (i - 1)
				jitter := time.Duration(rand.Int63n(int64(delay / 2)))
				delay += jitter
				timer := time.NewTimer(delay)

				select {
				case <-ctx.Done():
					timer.Stop()
					fmt.Println("Finishing retries and stopping reading process")
					done <- 1
					return
				case <-timer.C:
				}

				newVal := rand.Float32()
				if newVal <= 0.8 {
					fmt.Printf("Attempt %d to retry failed!\n", i)
					if i == 5 {
						fmt.Printf("All 5 attempts were failed! Sending message to DLQ!\n")
						err := dlqWriter.WriteMessages(ctx, kafka.Message{
							Value: []byte(msg.Value),
							Headers: []kafka.Header{
								{Key: "original_topic", Value: []byte(msg.Topic)},
								{Key: "original_partition", Value: []byte(strconv.Itoa(msg.Partition))},
								{Key: "original_offset", Value: []byte(strconv.FormatInt(msg.Offset, 10))},
								{Key: "attempts", Value: []byte(strconv.Itoa(i))},
								{Key: "failed_at", Value: []byte(time.Now().Format(time.RFC3339))},
							},
						})
						if err != nil {
							fmt.Println("Failed to write message to DLQ!")
							break
						}
						fmt.Printf("Message sent to DLQ: %s", string(msg.Value))
						dlqed = true
						break
					}
					continue
				}

				fmt.Printf("Successful attempt to retry %d\n", i)
				processed = true
				break
			}
		} else {
			processed = true
		}

		if processed {
			fmt.Printf("%s\n", string(msg.Value))
		}

		if processed || dlqed {
			err := r.CommitMessages(ctx, msg)
			if err != nil {
				fmt.Println("Failed to commit message")
				continue
			}
			fmt.Printf("Message committed\n\n")
		}
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
	dlqTopik := os.Getenv("KAFKA_DLQ_TOPIC")
	groupid := "consumer-group-id10"

	baseDelayStr := os.Getenv("KAFKA_BASE_DELAY")
	baseDelay, err := time.ParseDuration(baseDelayStr)
	if err != nil {
		panic(err)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		GroupID:     groupid,
		Topic:       topic,
		StartOffset: kafka.LastOffset,
	})
	defer func() {
		r.Close()
	}()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   dlqTopik,
	})
	defer func() {
		dlqWriter.Close()
	}()

	done := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go readMessages(ctx, done, r, dlqWriter, baseDelay)

	<-sigCh
	cancel()

	<-done
	fmt.Print("Stopped consumer")
}
