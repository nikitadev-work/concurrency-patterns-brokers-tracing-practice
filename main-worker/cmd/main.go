package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/jackc/pgx"
	"github.com/joho/godotenv"

	postgrespkg "study/main-worker/internal"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("error env load")
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

	log.Println("Main-worker started")
	go postgrespkg.AddNewLine(done, ctx, db)

	<-sigCh
	cancel()

	<-done
	log.Println("Main worker stopped")
}
