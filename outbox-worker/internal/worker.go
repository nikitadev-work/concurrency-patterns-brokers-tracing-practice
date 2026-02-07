package internal

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx"
	"github.com/segmentio/kafka-go"
)

type OutboxModel struct {
	Id            int64
	AggregateType string
	AggregateId   uuid.UUID
	EventType     string
	Payload       []byte
}

func Work(db *pgx.Conn, done chan int, ctx context.Context, w *kafka.Writer) error {
	for {
		tx, err := db.Begin()
		if err != nil {
			log.Println("Begin transaction error")
			done <- 1
			return err
		}

		rows, err := tx.Query(
			"SELECT id, aggregate_type, aggregate_id, event_type, payload FROM outbox " +
				"WHERE status = 'NEW' " +
				"ORDER BY created_at " +
				"LIMIT 10 " +
				"FOR UPDATE SKIP LOCKED",
		)
		if err != nil {
			log.Println("Select from outbox error")
			done <- 1
			tx.Rollback()
			return err
		}

		var res []OutboxModel
		var ids []int64
		for rows.Next() {
			var row OutboxModel
			err := rows.Scan(
				&row.Id,
				&row.AggregateType,
				&row.AggregateId,
				&row.EventType,
				&row.Payload,
			)
			if err != nil {
				log.Println("Rows scan error")
				done <- 1
				tx.Rollback()
				return err
			}

			res = append(res, row)
			ids = append(ids, row.Id)
		}
		rows.Close()

		if len(ids) != 0 {
			_, err = tx.Exec(
				"UPDATE outbox SET status = 'LOCKED' WHERE id = ANY($1)",
				ids,
			)
			if err != nil {
				log.Println("Update rows error")
				done <- 1
				tx.Rollback()
				return err
			}

			err = tx.Commit()
			if err != nil {
				log.Println("Commit transaction error")
				tx.Rollback()
				return err
			}

			for _, r := range res {
				err := w.WriteMessages(ctx, kafka.Message{
					Key:   []byte(r.AggregateId.String()),
					Value: r.Payload,
					Headers: []kafka.Header{
						{
							Key:   "event_id",
							Value: []byte(strconv.FormatInt(r.Id, 10)),
						},
					},
				})
				if err != nil {
					log.Println("Write message error")
					_, err = db.Exec(
						"UPDATE outbox SET status = 'NEW' WHERE id = $1",
						r.Id,
					)
					log.Println("Status NEW returned")
					done <- 1
					return err
				}

				t := time.Now()
				_, err = db.Exec(
					"UPDATE outbox SET status = 'PUBLISHED', published_at = $1 WHERE id = $2",
					t,
					r.Id,
				)
				if err != nil {
					log.Println("Update status error")
					done <- 1
					return err
				}
			}
		} else {
			err = tx.Commit()
			if err != nil {
				log.Println("Commit transaction error")
				tx.Rollback()
				return err
			}
		}

		t := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			done <- 1
			log.Println("Got exit signal. Stopping...")
			return nil
		case <-t.C:
		}
	}
}
