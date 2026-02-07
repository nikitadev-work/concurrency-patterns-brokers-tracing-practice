package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

func InitTopics(ctx context.Context, bootstap string, configs []kafka.TopicConfig) error {
	fmt.Println("Starting topics creation")

	dialer := &kafka.Dialer{
		Timeout: 10 * time.Second,
	}

	conn, err := dialer.DialContext(ctx, "tcp", bootstap)
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	err = conn.CreateTopics(configs...)
	if err != nil {
		return fmt.Errorf("error : %w", err)
	}

	fmt.Println("All topic created")
	return nil
}
