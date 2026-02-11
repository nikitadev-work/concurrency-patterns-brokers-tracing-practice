package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	redispkg "study/consumer/internal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"log/slog"

	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

var (
	processedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_processed_total",
		Help: "Total number of successfully processed messages (committed).",
	})

	errorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "consumer_errors_total",
		Help: "Total number of processing errors.",
	})

	processingSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_processing_seconds",
		Help:    "Message processing time in seconds.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	})

	messageAge = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "consumer_message_age_seconds",
		Help:    "Message age in seconds.",
		Buckets: []float64{0.5, 1, 2, 5, 10, 30, 60, 300},
	})
)

type kafkaHeaderCarrier struct {
	headers *[]kafka.Header
}

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range *c.headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(key, val string) {
	*c.headers = append(*c.headers, kafka.Header{
		Key:   key,
		Value: []byte(val),
	})
}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(*c.headers))
	for _, h := range *c.headers {
		keys = append(keys, h.Key)
	}
	return keys
}

func simpleRead(ctx context.Context, done chan int, r *kafka.Reader, rds *redis.Client, logger *slog.Logger) {
	tr := otel.Tracer("consumer")

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown: stop reading")
			done <- 1
			return
		default:
		}

		msg, err := r.FetchMessage(ctx)
		if err != nil {
			logger.Error("Failed to read message.")
			continue
		}

		var traceId string

		for _, v := range msg.Headers {
			if v.Key == "trace_id" {
				traceId = string(v.Value)
			}
		}

		var ctxMsg context.Context
		var span trace.Span

		if traceId != "" {
			tid, err := trace.TraceIDFromHex(traceId)
			if err == nil {
				sc := trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: tid,
					Remote:  true,
				})
				parentCtx := trace.ContextWithRemoteSpanContext(ctx, sc)
				ctxMsg, span = tr.Start(parentCtx, "consume_message")
				logger.Info("CONSUMER trace_id", "trace_id", span.SpanContext().TraceID().String())
			} else {
				ctxMsg, span = tr.Start(ctx, "consume_message")
			}
		} else {
			ctxMsg, span = tr.Start(ctx, "consume_message")
		}

		span.SetAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", msg.Topic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
		)

		start := time.Now()

		var eventId string
		var createdAt int64

		for _, v := range msg.Headers {
			if v.Key == "event_id" {
				eventId = string(v.Value)
			}
			if v.Key == "created_at" {
				createdAt, _ = strconv.ParseInt(string(v.Value), 10, 64)
			}
		}

		if eventId == "" {
			logger.Error("Empty event_id")
			span.RecordError(errors.New("empty event_id"))
			span.End()
			continue
		}

		age := float64(time.Now().UnixMilli()-createdAt) / 1000.0

		err = redispkg.AddNewKey(ctxMsg, rds, eventId, 1)
		if err != nil {
			if errors.Is(err, redispkg.ErrKeyAlreadyExists) {
				logger.Info("dedup hit: skip side-effect")
			} else {
				errorsTotal.Inc()
				span.RecordError(err)
				span.End()
				continue
			}
		}

		t := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			t.Stop()
			done <- 1
			log.Println("Got exit signal. Stopping...")
		case <-t.C:
		}

		err = r.CommitMessages(ctxMsg, msg)
		if err != nil {
			logger.Info("Commit message error")
			span.RecordError(err)
			span.End()
			continue
		}

		processingSeconds.Observe(time.Since(start).Seconds())
		messageAge.Observe(age)
		processedTotal.Inc()

		span.End()
	}

}

func initTracer(ctx context.Context) (*sdktrace.TracerProvider, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4317"
	}

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("consumer"),
			attribute.String("env", "local"),
		),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	return tp, nil
}

func main() {
	fmt.Println("Consumer started")

	if err := godotenv.Load(); err != nil {
		fmt.Println("Failed to load .env")
		panic(err)
	}

	l := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	l = l.With("service", "consumer")
	l.Info("starting")

	redisAddr := os.Getenv("REDIS_ADDR")
	rds := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() {
		rds.Close()
	}()

	broker := os.Getenv("KAFKA_BROKER")
	groupid := "consumer-group-id10"

	topic := "study.main"
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupid,
		StartOffset: kafka.LastOffset,
	})
	defer func() {
		r.Close()
	}()

	done := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	tp, err := initTracer(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(c)
	}()

	otel.SetTextMapPropagator(propagation.TraceContext{})

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		addr := ":2112"
		fmt.Println("metrics listening on", addr)
		l.Info("metrics listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			fmt.Println("metrix server error", err)
		}
	}()

	go simpleRead(ctx, done, r, rds, l)

	<-sigCh
	l.Info("shutdown: signal received")
	cancel()

	<-done
	l.Info("stopped")
}
