package postgrespkg

import (
	"context"
	"log"

	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx"
)

type OrderModel struct {
	Id     uuid.UUID
	Status string
	Amount int64
}

type OutboxModel struct {
	AggregateType string
	AggregateId   uuid.UUID
	EventType     string
	Payload       []byte
	Status        string
}

type Payload struct {
	Status string `json:"status"`
	Amount int64  `json:"amount"`
}

func AddNewLine(done chan int, ctx context.Context, db *pgx.Conn) {
	i := 0

	for {
		select {
		case <-ctx.Done():
			log.Println("Got exit signal. Finishing...")
			done <- 1
			return
		default:
		}

		payload := Payload{
			Status: "Active",
			Amount: int64(i),
		}
		newId, _ := uuid.NewUUID()
		newOrder := OrderModel{
			Id:     newId,
			Status: payload.Status,
			Amount: payload.Amount,
		}

		p, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error while marshalling json")
			return
		}

		newOuboxLine := OutboxModel{
			AggregateType: "",
			AggregateId:   newId,
			EventType:     "order",
			Payload:       p,
			Status:        "NEW",
		}

		tx, err := db.Begin()
		if err != nil {
			log.Printf("Error begin transaction")
			tx.Rollback()
			return
		}

		_, err = tx.Exec(
			"INSERT INTO orders (id, status, amount) VALUES ($1, $2, $3)",
			newOrder.Id,
			newOrder.Status,
			newOrder.Amount,
		)
		if err != nil {
			log.Println("Order insertion error")
			tx.Rollback()
			return
		}
		_, err = tx.Exec(
			"INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload, status) VALUES ($1, $2, $3, $4, $5)",
			newOuboxLine.AggregateType,
			newOuboxLine.AggregateId,
			newOuboxLine.EventType,
			newOuboxLine.Payload,
			newOuboxLine.Status,
		)
		if err != nil {
			log.Println("Outbox insertion error")
			tx.Rollback()
			return
		}

		err = tx.Commit()
		if err != nil {
			log.Println("Commit transaction error")
			tx.Rollback()
			return
		}

		// t := time.NewTimer(1 * time.Second)
		// select {
		// case <-ctx.Done():
		// 	t.Stop()
		// 	done <- 1
		// 	log.Println("Context finished, stopping...")
		// 	return
		// case <-t.C:
		// }
		i++
	}
}
