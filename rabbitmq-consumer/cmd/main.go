package main

import (
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

	err = ch.Qos(1, 0, false)
	if err != nil {
		log.Fatal(err)
	}

	_, err = ch.QueueDeclare(
		"jobs",
		true, // durable
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"jobs",
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("consumer started, waiting messages...")

	for msg := range msgs {
		log.Printf("got: %s", string(msg.Body))

		time.Sleep(300 * time.Millisecond)

		if err := msg.Ack(false); err != nil {
			log.Println("ack error")
		}
	}
}
