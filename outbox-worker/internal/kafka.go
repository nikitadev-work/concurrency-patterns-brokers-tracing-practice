package internal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type TopicsConfig struct {
	Prefix   string            `yaml:"prefix"`
	Defaults DefaultsConfig    `yaml:"defaults"`
	Topics   []TopicConfig     `yaml:"topics"`
	Retry    RetryTopicsConfig `yaml:"retryTopics"`
}

type DefaultsConfig struct {
	Partitions        int `yaml:"partitions"`
	ReplicationFactor int `yaml:"replicationFactor"`
}

type TopicConfig struct {
	FullName    string `yaml:"fullName"`
	RetentionMs int64  `yaml:"retentionMs"`
}

type RetryTopicsConfig struct {
	Delays      []string `yaml:"delays"`
	RetentionMs int64    `yaml:"retentionMs"`
}

func CreateTopicSpecs(cfg TopicsConfig) []kafka.TopicConfig {
	p := cfg.Prefix

	var specs []kafka.TopicConfig

	for _, t := range cfg.Topics {
		name := strings.ReplaceAll(t.FullName, "{prefix}", p)
		specs = append(specs, kafka.TopicConfig{
			Topic:             name,
			ReplicationFactor: cfg.Defaults.ReplicationFactor,
			NumPartitions:     cfg.Defaults.Partitions,
		})
	}

	for _, d := range cfg.Retry.Delays {
		name := fmt.Sprintf("%s.retry.%s", p, d)
		specs = append(specs, kafka.TopicConfig{
			Topic:             name,
			ReplicationFactor: cfg.Defaults.ReplicationFactor,
			NumPartitions:     cfg.Defaults.Partitions,
		})
	}

	return specs
}

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

		value := fmt.Sprintf("New message %d from producer!", i)
		msg := []byte(value)
		err := w.WriteMessages(ctx, kafka.Message{
			Value: msg,
		})
		if err != nil {
			panic(err)
		}
		fmt.Printf("New message %d sent!\n", i)
		i++
		time.Sleep(1 * time.Second)
	}
}
