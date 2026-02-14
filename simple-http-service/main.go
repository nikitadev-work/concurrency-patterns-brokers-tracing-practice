package main

import (
	"log"
	"net/http"
)

func HealthcheckHandler(writer http.ResponseWriter, reader *http.Request) {
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("Ok"))
	log.Println("New GET request")
}

func main() {
	http.HandleFunc("/health", HealthcheckHandler)
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("server failed")
	}
}
