package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"study/outbox-worker/internal"
	"syscall"

	"github.com/jackc/pgx"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	err := godotenv.Load()
	if err != nil {
		fmt.Println("Failed to load .env")
		panic(err)
	}

	broker := os.Getenv("KAFKA_BROKER")

	file, err := os.ReadFile("kafka-topics.yml")
	if err != nil {
		fmt.Println("Can not read the config file")
		panic(err)
	}

	var cfg internal.TopicsConfig
	err = yaml.Unmarshal(file, &cfg)
	if err != nil {
		fmt.Println("Error while unmarshalling config")
		panic(err)
	}

	res := internal.CreateTopicSpecs(cfg)
	err = internal.InitTopics(ctx, broker, res)
	if err != nil {
		fmt.Println("Error while creating topics")
		panic(err)
	}

	pgDb := os.Getenv("POSTGRES_DB")
	pgUser := os.Getenv("POSTGRES_USER")
	pgPass := os.Getenv("POSTGRES_PASSWORD")
	pgHost := os.Getenv("POSTGRES_HOST")
	pgPort := os.Getenv("POSTGRES_PORT")

	port, err := strconv.Atoi(pgPort)
	if err != nil {
		log.Fatal(err)
	}

	db, err := pgx.Connect(pgx.ConnConfig{
		Host:     pgHost,
		Port:     uint16(port),
		User:     pgUser,
		Password: pgPass,
		Database: pgDb,
	})

	topic := "study.main"
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}
	defer func() {
		w.Close()
	}()

	log.Println("Outbox worker started")
	go internal.Work(db, done, ctx, w)

	<- sigCh
	cancel()

	<- done
	log.Println("Outbox worker finished")
}
