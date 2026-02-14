FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o app ./simple-http-service

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /app/app /app/app
EXPOSE 8080
CMD ["/app/app"]
