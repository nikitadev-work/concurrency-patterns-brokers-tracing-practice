package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	err = ch.Confirm(false)
	if err != nil {
		log.Fatal(err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	_, err = ch.QueueDeclare(
		"jobs",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; ; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		body := []byte(time.Now().Format(time.RFC3339Nano))
		err := ch.PublishWithContext(ctx, "", "jobs", false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
			Timestamp:    time.Now(),
		})
		cancel()
		if err != nil {
			log.Printf("publish error: %v", err)
			continue
		}

		// 2) ждём confirm именно для этого publish
		select {
		case c := <-confirms:
			if !c.Ack {
				log.Printf("broker nacked publish")
				continue
			}
			log.Printf("confirmed #%d", i)
		case <-time.After(5 * time.Second):
			log.Printf("confirm timeout")
			continue
		}

		time.Sleep(200 * time.Millisecond)
	}
}
